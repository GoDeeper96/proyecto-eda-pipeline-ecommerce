"""
Genera la version enriquecida del dataset REES46 (2019-Nov) para la Opcion B.

Principio: toda columna nueva se DERIVA de los datos reales con una regla documentada.
Lo unico aleatorio (semilla fija) es la suciedad inyectada para las etapas 0-1:
duplicados, filas corruptas y prefijos/errores de tipeo en las busquedas.

- Lee 2019-Nov.csv.zip en streaming (no lo modifica: queda como version original).
- Toma las primeras N filas (por defecto 10M, orden temporal original).
- Pasada 1: mide sobre los datos reales demanda por producto y actividad anomala por usuario.
- Pasada 2: escribe 2019-Nov-enriquecido.zip con:
    2019-Nov-enriquecido.csv   -> stream con columnas nuevas
    blacklist.csv              -> cuentas senaladas (historico) para el Bloom Filter
    diccionario_columnas.md    -> descripcion de cada columna y reglas de derivacion

Uso:
    python scripts/generar_dataset_enriquecido.py [--n 10000000] [--seed 42]
"""
import argparse
import csv
import io
import math
import random
import time
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SRC_ZIP = BASE / "2019-Nov.csv.zip"
SRC_NAME = "2019-Nov.csv"
OUT_ZIP = BASE / "2019-Nov-enriquecido.zip"
OUT_NAME = "2019-Nov-enriquecido.csv"

# Reglas de derivacion (documentadas en diccionario_columnas.md)
SUSP_PURCHASES_PER_HOUR = 5     # >= 5 compras en una misma hora (p99.9 real ~ 8)
SUSP_EVENTS_PER_MINUTE = 10     # >= 10 eventos en un mismo minuto (p99.99 real ~ 11)
BLACKLIST_CUTOFF = "2019-11-03"  # anomalias detectadas antes de esta fecha = historico senalado
BULKY_PREFIXES = ("furniture", "construction", "country_yard",
                  "appliances.kitchen.refrigerators", "appliances.kitchen.washer",
                  "appliances.kitchen.oven", "appliances.kitchen.dishwasher",
                  "appliances.kitchen.hood", "appliances.environment.air_conditioner")
SCHEDULED_DELAY_DAYS = 2        # entrega programada: +2 dias a las 09:00
STOCK_MARGIN = 0.10             # stock inicial = demanda real del periodo * 1.10

# Suciedad inyectada (aleatoria, semilla fija)
DUPLICATE_P = 0.01
DUPLICATE_DELAY = (1, 50)
INVALID_P = 0.001
SEARCH_P = 0.10

HEADER_IN = ["event_time", "event_type", "product_id", "category_id", "category_code",
             "brand", "price", "user_id", "user_session"]
HEADER_OUT = ["event_id"] + HEADER_IN + [
    "order_id", "order_type", "scheduled_time", "stock", "search_query",
    "is_suspicious", "is_duplicate", "is_invalid",
]


def leer_filas(n):
    """Generador de filas originales (lista de 9 campos) desde el zip, en streaming."""
    with zipfile.ZipFile(SRC_ZIP) as zin, zin.open(SRC_NAME) as fb:
        reader = csv.reader(io.TextIOWrapper(fb, encoding="utf-8", newline=""))
        header = next(reader)
        assert header == HEADER_IN, header
        for i, row in enumerate(reader):
            if i >= n:
                break
            yield row


def pasada_1(n):
    """Mide sobre los datos reales: demanda por producto y usuarios con actividad anomala."""
    demanda = Counter()
    first_anomaly = {}          # user_id -> ventana (minuto u hora) de la primera anomalia
    reasons = {}                # user_id -> motivo
    cur_min, per_min = None, Counter()
    cur_hour, per_hour = None, Counter()

    def cerrar(counter, umbral, motivo, ventana):
        for u, c in counter.items():
            if c >= umbral and u not in first_anomaly:
                first_anomaly[u] = ventana
                reasons[u] = motivo

    for row in leer_filas(n):
        t, e, p, u = row[0], row[1], row[2], row[7]
        m, h = t[:16], t[:13]
        if m != cur_min:
            cerrar(per_min, SUSP_EVENTS_PER_MINUTE, "eventos_por_minuto", cur_min)
            per_min.clear()
            cur_min = m
        per_min[u] += 1
        if e == "purchase":
            demanda[p] += 1
            if h != cur_hour:
                cerrar(per_hour, SUSP_PURCHASES_PER_HOUR, "compras_por_hora", cur_hour)
                per_hour.clear()
                cur_hour = h
            per_hour[u] += 1
    cerrar(per_min, SUSP_EVENTS_PER_MINUTE, "eventos_por_minuto", cur_min)
    cerrar(per_hour, SUSP_PURCHASES_PER_HOUR, "compras_por_hora", cur_hour)
    return demanda, first_anomaly, reasons


class Enriquecedor:
    def __init__(self, seed, demanda, sospechosos):
        self.rng = random.Random(seed)
        self.demanda = demanda
        self.sospechosos = sospechosos
        self.stock = {}
        self.carted = set()     # sesiones que ya agregaron algo al carrito
        self.orders = {}        # user_session -> (order_id, order_type, scheduled_time)
        self.order_types = Counter()

    def order_for(self, row):
        session = row[8]
        o = self.orders.get(session)
        if o is None:
            sched = ""
            if row[4].startswith(BULKY_PREFIXES):
                otype = "scheduled"
                t = datetime.strptime(row[0][:10], "%Y-%m-%d") + timedelta(days=SCHEDULED_DELAY_DAYS, hours=9)
                sched = t.strftime("%Y-%m-%d %H:%M:%S UTC")
            elif session in self.carted:
                otype = "standard"
            else:
                otype = "express"
            o = ("ORD-%08d" % (len(self.orders) + 1), otype, sched)
            self.orders[session] = o
            self.order_types[otype] += 1
        return o

    def stock_for(self, pid, purchase):
        s = self.stock.get(pid)
        if s is None:
            s = max(1, math.ceil(self.demanda.get(pid, 0) * (1 + STOCK_MARGIN)))
        if purchase and s > 0:
            s -= 1
        self.stock[pid] = s
        return s

    def search_query(self, row):
        brand, cat = row[5], row[4]
        options = [x for x in (brand, cat.split(".")[-1] if cat else "") if x]
        if not options:
            return ""
        q = self.rng.choice(options).replace("_", " ").lower()
        r = self.rng.random()
        if r < 0.3 and len(q) > 3:      # el usuario escribe solo un prefijo (autocompletado)
            q = q[: self.rng.randint(2, len(q) - 1)]
        elif r < 0.4 and len(q) > 3:    # error de tipeo (transposicion)
            i = self.rng.randrange(len(q) - 1)
            q = q[:i] + q[i + 1] + q[i] + q[i + 2:]
        return q

    def corrupt(self, row):
        row = list(row)
        k = self.rng.randrange(4)
        if k == 0:
            row[6] = "-" + row[6]               # precio negativo
        elif k == 1:
            row[7] = ""                         # user_id vacio
        elif k == 2:
            row[0] = "2019-13-45 99:99:99 UTC"  # timestamp invalido
        else:
            row[1] = row[1].upper() + " "       # event_type mal formateado
        return row

    def extra(self, row):
        etype = row[1]
        if etype == "cart":
            self.carted.add(row[8])
        order_id = otype = sched = query = ""
        if etype == "purchase":
            order_id, otype, sched = self.order_for(row)
        elif etype == "view" and self.rng.random() < SEARCH_P:
            query = self.search_query(row)
        stock = self.stock_for(row[2], etype == "purchase")
        susp = 1 if row[7] in self.sospechosos else 0
        return [order_id, otype, sched, stock, query, susp]


def entrada_zip(name):
    info = zipfile.ZipInfo(name, date_time=time.localtime()[:6])
    info.compress_type = zipfile.ZIP_DEFLATED
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10_000_000, help="filas originales a procesar")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=OUT_ZIP)
    args = ap.parse_args()
    t0 = time.time()

    print("Pasada 1: midiendo patrones reales...", flush=True)
    demanda, first_anomaly, reasons = pasada_1(args.n)
    blacklist = sorted(u for u, t in first_anomaly.items() if t < BLACKLIST_CUTOFF)
    print(f"  sospechosos: {len(first_anomaly):,}  en blacklist historica: {len(blacklist):,}"
          f"  ({time.time() - t0:.0f}s)", flush=True)

    print("Pasada 2: escribiendo dataset enriquecido...", flush=True)
    enr = Enriquecedor(args.seed, demanda, set(first_anomaly))
    rng = enr.rng
    stats = Counter()
    pending = []  # duplicados programados: (fila_de_salida_en_la_que_aparece, fila)
    event_id = 0

    with zipfile.ZipFile(args.out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zout:
        with zout.open(entrada_zip(OUT_NAME), "w", force_zip64=True) as fout_b:
            fout = io.TextIOWrapper(fout_b, encoding="utf-8", newline="")
            writer = csv.writer(fout, lineterminator="\n")
            writer.writerow(HEADER_OUT)

            def write(r):
                nonlocal event_id
                event_id += 1
                r[0] = event_id
                writer.writerow(r)
                stats["out"] += 1
                stats["suspicious_rows"] += r[-3]

            for row in leer_filas(args.n):
                stats["orig"] += 1
                r = [None] + row + enr.extra(row) + [0, 0]
                if rng.random() < INVALID_P:
                    r[1:10] = enr.corrupt(r[1:10])
                    r[-1] = 1
                    stats["invalid"] += 1
                write(r)
                if rng.random() < DUPLICATE_P:
                    d = list(r)
                    d[-2] = 1
                    pending.append((stats["out"] + rng.randint(*DUPLICATE_DELAY), d))
                if pending:
                    keep = []
                    for when, d in pending:
                        if when <= stats["out"]:
                            write(d)
                            stats["duplicate"] += 1
                        else:
                            keep.append((when, d))
                    pending = keep
                if stats["orig"] % 1_000_000 == 0:
                    print(f"  {stats['orig']:,} filas  ({time.time() - t0:.0f}s)", flush=True)
            for _, d in pending:
                write(d)
                stats["duplicate"] += 1
            fout.flush()
            fout.detach()

        bl = io.StringIO()
        w = csv.writer(bl, lineterminator="\n")
        w.writerow(["entity_type", "entity_value", "first_anomaly_window", "reason"])
        for u in blacklist:
            w.writerow(["account", u, first_anomaly[u], reasons[u]])
        zout.writestr(entrada_zip("blacklist.csv"), bl.getvalue())
        zout.writestr(entrada_zip("diccionario_columnas.md"),
                      diccionario(args, stats, enr, first_anomaly, reasons, blacklist))

    print("Listo:", args.out)
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(f"  ordenes: {len(enr.orders):,}  tipos: {dict(enr.order_types)}")
    print(f"  tiempo: {time.time() - t0:.0f}s")


def diccionario(args, stats, enr, first_anomaly, reasons, blacklist):
    n_ord = len(enr.orders) or 1
    tipos = " / ".join(f"{k} {v:,} ({100 * v / n_ord:.1f}%)" for k, v in enr.order_types.most_common())
    motivos = Counter(reasons.values())
    return f"""# Diccionario de columnas: 2019-Nov-enriquecido.csv

Fuente: REES46 "eCommerce behavior data from multi category store", archivo `2019-Nov.csv`
(la version original se conserva intacta en `2019-Nov.csv.zip`).

Generado con `scripts/generar_dataset_enriquecido.py --n {args.n} --seed {args.seed}`.

**Principio:** las columnas nuevas se derivan de los datos reales con reglas deterministas.
Solo la suciedad (duplicados, filas corruptas, prefijos/typos de busqueda) es aleatoria con semilla fija.

| Metrica | Valor |
|---|---|
| Filas originales procesadas | {stats['orig']:,} |
| Filas de salida | {stats['out']:,} |
| Duplicados inyectados | {stats['duplicate']:,} |
| Filas corruptas inyectadas | {stats['invalid']:,} |
| Ordenes | {len(enr.orders):,}: {tipos} |
| Usuarios sospechosos | {len(first_anomaly):,} (compras_por_hora {motivos['compras_por_hora']:,}, eventos_por_minuto {motivos['eventos_por_minuto']:,}) |
| Filas de usuarios sospechosos | {stats['suspicious_rows']:,} |
| Cuentas en blacklist historica | {len(blacklist):,} |

## Columnas originales

| Columna | Descripcion |
|---|---|
| event_time | Momento del evento (UTC) |
| event_type | view, cart, purchase |
| product_id | Id del producto |
| category_id | Id de categoria |
| category_code | Taxonomia de categoria (puede venir vacia, ~32%) |
| brand | Marca (puede venir vacia, ~15%) |
| price | Precio |
| user_id | Id del usuario (cuenta) |
| user_session | Id de sesion |

## Columnas agregadas

| Columna | Etapa | Tipo | Regla |
|---|---|---|---|
| event_id | 0, 1 | Generada | Correlativo unico por fila de salida (los duplicados reciben uno nuevo) |
| order_id | 2, 8 | Derivada | Una orden por `user_session` con compra; todas las compras de esa sesion comparten la orden |
| order_type | 2 | Derivada | `scheduled` si el primer producto comprado es voluminoso (categoria en {', '.join(BULKY_PREFIXES)}); `standard` si la sesion paso por carrito antes de comprar; `express` si es compra directa sin carrito ("comprar ahora") |
| scheduled_time | 2 | Derivada | Solo `scheduled`: fecha de compra + {SCHEDULED_DELAY_DAYS} dias, 09:00 UTC |
| stock | 3 | Derivada | Stock inicial = ceil(compras reales del producto en el periodo x {1 + STOCK_MARGIN:.2f}), minimo 1; baja 1 por compra |
| search_query | 9 | Derivada + ruido | {int(SEARCH_P * 100)}% de las vistas: marca o ultima parte de la categoria del producto visto; 30% solo prefijo, 10% con transposicion de letras |
| is_suspicious | eval. 6 | Ground truth derivado | 1 si la cuenta tuvo >= {SUSP_PURCHASES_PER_HOUR} compras en una misma hora o >= {SUSP_EVENTS_PER_MINUTE} eventos en un mismo minuto (conteo exacto, ventanas fijas) en algun momento del periodo |
| is_duplicate | eval. 1 | Ruido inyectado | 1 si es un duplicado inyectado ({DUPLICATE_P * 100:.0f}%, aparece {DUPLICATE_DELAY[0]}-{DUPLICATE_DELAY[1]} filas despues, mismo contenido, nuevo event_id) |
| is_invalid | eval. 1 | Ruido inyectado | 1 si la fila fue corrompida ({INVALID_P * 100:.1f}%: precio negativo, user_id vacio, timestamp invalido o event_type mal formado) |

**Importante:** `is_suspicious`, `is_duplicate` e `is_invalid` son etiquetas de evaluacion. El pipeline NO debe
leerlas para decidir; solo se usan para medir aciertos, falsos positivos y falsos negativos de las etapas 1 y 6.

Nota: el dataset original ya trae duplicados exactos naturales (~0.04%), no marcados en `is_duplicate`.

## blacklist.csv

Cuentas (`user_id`) cuya primera ventana anomala ocurrio antes de {BLACKLIST_CUTOFF} (historico de cuentas ya
senaladas). Se carga en el Bloom Filter. Las cuentas sospechosas que aparecen despues no estan en la lista y
debe detectarlas el Count-Min Sketch por intensidad de actividad en ventana. Un positivo del Bloom se verifica
contra la lista exacta antes de bloquear.
"""


if __name__ == "__main__":
    main()
