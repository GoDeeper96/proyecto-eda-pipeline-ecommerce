"""Mediciones complementarias de las etapas 0-3: estructura elegida frente a su alternativa.

Usa datos sintéticos con semilla 42 y los mismos tamaños del pipeline
(buffer de 20,000 eventos, lotes de 10,000, ~26,800 órdenes en el heap, 133,905 productos).
Uso: python scripts/benchmark_alternativas_etapas_0_5.py
"""
import bisect
import heapq
import platform
import random
import statistics
import time
from collections import deque

random.seed(42)
REPS = 10


def medir(f):
    tiempos = []
    for _ in range(REPS):
        inicio = time.perf_counter()
        f()
        tiempos.append(time.perf_counter() - inicio)
    return statistics.mean(tiempos), statistics.stdev(tiempos)


def busqueda_binaria(lista, objetivo):
    izquierda, derecha = 0, len(lista) - 1
    while izquierda <= derecha:
        medio = (izquierda + derecha) // 2
        if lista[medio] == objetivo:
            return medio
        if lista[medio] < objetivo:
            izquierda = medio + 1
        else:
            derecha = medio - 1
    return -1


def buscar_lineal(lista, objetivo):
    for i, x in enumerate(lista):
        if x == objetivo:
            return i
    return -1


# Etapa 0: 1,000,000 eventos por un buffer de 20,000; sale un lote de 10,000 cuando se llena.
N_STREAM, CAP, LOTE = 1_000_000, 20_000, 10_000


def buffer_deque():
    b = deque()
    for e in range(N_STREAM):
        b.append(e)
        if len(b) == CAP:
            [b.popleft() for _ in range(LOTE)]


def buffer_lista():
    b = []
    for e in range(N_STREAM):
        b.append(e)
        if len(b) == CAP:
            [b.pop(0) for _ in range(LOTE)]


# Etapa 1: 10,000 claves de un lote contra las 10,000 claves ordenadas del lote anterior.
claves = sorted(f"2019-11-01 {random.random():.12f}|view|{random.randrange(10**7)}" for _ in range(10_000))
consultas = random.sample(claves, 5_000) + [f"x{i}" for i in range(5_000)]   # mitad presentes


def dedup_binaria():
    for c in consultas:
        busqueda_binaria(claves, c)


def dedup_lineal():
    for c in consultas[:1_000]:   # 1,000 consultas; se escala x10 al reportar
        buscar_lineal(claves, c)


# Etapa 2: heap con 26,800 órdenes pendientes; 10,000 inserciones y 10,000 extracciones intercaladas.
base = [(random.randint(2, 4), i) for i in range(26_800)]
nuevas = [(random.randint(2, 4), 26_800 + i) for i in range(10_000)]


def con_heap():
    h = base.copy()
    heapq.heapify(h)
    for x in nuevas:
        heapq.heappush(h, x)
        heapq.heappop(h)


def con_lista_ordenada():
    h = sorted(base)
    for x in nuevas:
        bisect.insort(h, x)
        h.pop(0)


def con_lista_sin_orden():
    h = base.copy()
    for x in nuevas[:1_000]:   # 1,000 operaciones; se escala x10 al reportar
        h.append(x)
        h.remove(min(h))


# Etapa 3: 10,000 consultas puntuales sobre 133,905 productos.
ids = random.sample(range(1_000_000, 100_000_000), 133_905)
tabla = {p: {"precio": 1.0} for p in ids}
ids_ordenados = sorted(ids)
consultas_p = random.choices(ids, k=10_000)


def lookup_hash():
    for p in consultas_p:
        tabla[p]


def lookup_binaria():
    for p in consultas_p:
        busqueda_binaria(ids_ordenados, p)


def lookup_lineal():
    for p in consultas_p[:100]:   # 100 consultas; se escala x100 al reportar
        buscar_lineal(ids, p)


print("Plataforma:", platform.processor(), "|", platform.platform(), "| Python", platform.python_version())
print("Repeticiones:", REPS, "| semilla: 42\n")
for etapa, nombre, f, escala in [
    ("0", "deque (append/popleft)", buffer_deque, 1),
    ("0", "lista (append/pop(0))", buffer_lista, 1),
    ("1", "búsqueda binaria, 10,000 consultas", dedup_binaria, 1),
    ("1", "búsqueda lineal, 10,000 consultas", dedup_lineal, 10),
    ("2", "heap, 10,000 push+pop", con_heap, 1),
    ("2", "lista ordenada (insort + pop(0))", con_lista_ordenada, 1),
    ("2", "lista sin orden (min + remove)", con_lista_sin_orden, 10),
    ("3", "tabla hash, 10,000 consultas", lookup_hash, 1),
    ("3", "búsqueda binaria en arreglo ordenado", lookup_binaria, 1),
    ("3", "búsqueda lineal", lookup_lineal, 100),
]:
    m, s = medir(f)
    print(f"Etapa {etapa} | {nombre:40} | {m * escala * 1000:10.2f} ms ± {s * escala * 1000:.2f}")
