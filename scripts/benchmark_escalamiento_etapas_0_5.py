"""Escalamiento de las estructuras de las etapas 0-5 frente a su alternativa.

Mide el tiempo por operación para n = 1,000; 10,000; 100,000 y 1,000,000 con datos sintéticos
(semilla 42, 5 repeticiones). Las alternativas lentas se miden sobre menos operaciones (mínimo 20) y se
reporta el tiempo por operación; los tamaños inviables se omiten.
Salida: salidas/benchmark_escalamiento_etapas_0_5.csv
Uso: python scripts/benchmark_escalamiento_etapas_0_5.py
"""
import bisect
import csv
import heapq
import random
import statistics
import time
from collections import deque

import numpy as np
from BTrees.OOBTree import OOBTree
from sklearn.neighbors import KDTree, NearestNeighbors

random.seed(42)
rng = np.random.default_rng(42)
REPS = 5
TAMANOS = [1_000, 10_000, 100_000, 1_000_000]
filas = []


def medir(etapa, estructura, n, ops, f, preparar=None):
    tiempos = []
    if etapa == 5:
        f()   # calentamiento: la primera llamada a scikit-learn incluye la creación de hilos
    for _ in range(REPS):
        estado = preparar() if preparar else None   # la preparación (p. ej. copiar la lista) no se mide
        inicio = time.perf_counter()
        f(estado) if preparar else f()
        tiempos.append((time.perf_counter() - inicio) / ops * 1e6)   # µs por operación
    filas.append([etapa, estructura, n, ops, statistics.mean(tiempos), statistics.stdev(tiempos)])
    print(f"Etapa {etapa} | {estructura:28} | n={n:>9,} | {statistics.mean(tiempos):12.3f} µs/op")


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


for n in TAMANOS:
    # Etapa 0: buffer de capacidad n; cuando se llena sale la mitad. Operación = un evento.
    eventos = 2 * n

    def con_deque():
        b = deque()
        for e in range(eventos):
            b.append(e)
            if len(b) == n:
                for _ in range(n // 2):
                    b.popleft()

    def con_lista():
        b = []
        for e in range(eventos):
            b.append(e)
            if len(b) == n:
                for _ in range(n // 2):
                    b.pop(0)

    medir(0, "deque", n, eventos, con_deque)
    if n <= 100_000:
        medir(0, "lista con pop(0)", n, eventos, con_lista)

    # Etapa 1: buscar claves de un lote en las n claves ordenadas del lote anterior.
    claves = sorted(f"{random.random():.15f}" for _ in range(n))
    consultas = random.choices(claves, k=2_000)
    q_lineal = consultas[:max(20, min(2_000, 2_000_000 // n))]
    medir(1, "búsqueda binaria", n, len(consultas), lambda: [busqueda_binaria(claves, c) for c in consultas])
    medir(1, "búsqueda lineal", n, len(q_lineal), lambda: [buscar_lineal(claves, c) for c in q_lineal])

    # Etapa 2: n órdenes pendientes; operación = una inserción + una extracción del mínimo.
    base = [(random.randint(2, 4), i) for i in range(n)]
    nuevas = [(random.randint(2, 4), n + i) for i in range(10_000)]
    ordenada = sorted(base)
    heap_base = base.copy()
    heapq.heapify(heap_base)
    k_ord = max(20, min(10_000, 20_000_000 // n))
    k_sin = max(20, min(2_000, 2_000_000 // n))

    def con_heap(h):
        for x in nuevas:
            heapq.heappush(h, x)
            heapq.heappop(h)

    def con_ordenada(h):
        for x in nuevas[:k_ord]:
            bisect.insort(h, x)
            h.pop(0)

    def con_sin_orden(h):
        for x in nuevas[:k_sin]:
            h.append(x)
            h.remove(min(h))

    medir(2, "heap", n, len(nuevas), con_heap, heap_base.copy)
    medir(2, "lista ordenada", n, k_ord, con_ordenada, ordenada.copy)
    medir(2, "lista sin orden", n, k_sin, con_sin_orden, base.copy)

    # Etapa 3: consultas puntuales sobre n productos.
    ids = random.sample(range(10**9), n)
    tabla = {p: 1.0 for p in ids}
    ids_ordenados = sorted(ids)
    consultas_p = random.choices(ids, k=10_000)
    q_lin = consultas_p[:max(20, min(2_000, 2_000_000 // n))]
    medir(3, "tabla hash", n, len(consultas_p), lambda: [tabla[p] for p in consultas_p])
    medir(3, "búsqueda binaria", n, len(consultas_p), lambda: [busqueda_binaria(ids_ordenados, p) for p in consultas_p])
    medir(3, "búsqueda lineal", n, len(q_lin), lambda: [buscar_lineal(ids, p) for p in q_lin])

    # Etapa 4: consulta por rango de precio que devuelve ~100 productos.
    precios = rng.uniform(1, 1_000, n)
    arbol = OOBTree()
    arbol.update({float(p): i for i, p in enumerate(precios)})
    lista_precios = precios.tolist()
    ancho = 999 * 100 / n
    inicios = rng.uniform(1, 1_000 - ancho, 200).tolist()
    q_scan = inicios[:max(20, min(200, 20_000_000 // n))]
    medir(4, "B+Tree", n, len(inicios), lambda: [list(arbol.values(a, a + ancho)) for a in inicios])
    medir(4, "recorrer el catálogo", n, len(q_scan), lambda: [[p for p in lista_precios if a <= p <= a + ancho] for a in q_scan])

    # Etapa 5: 200 consultas de K = 5 vecinos en 4 dimensiones.
    X = rng.standard_normal((n, 4))
    Q = rng.standard_normal((200, 4))
    kd = KDTree(X)
    bruta = NearestNeighbors(n_neighbors=5, algorithm="brute").fit(X)
    medir(5, "KD-Tree", n, len(Q), lambda: kd.query(Q, k=5))
    medir(5, "fuerza bruta", n, len(Q), lambda: bruta.kneighbors(Q))

with open("salidas/benchmark_escalamiento_etapas_0_5.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["etapa", "estructura", "n", "operaciones", "us_por_op", "desv_us"])
    w.writerows(filas)
print("salidas/benchmark_escalamiento_etapas_0_5.csv")
