# Diccionario de columnas: 2019-Nov-enriquecido.csv

Fuente: REES46 "eCommerce behavior data from multi category store", archivo `2019-Nov.csv`
(la version original se conserva intacta en `2019-Nov.csv.zip`).

Generado con `scripts/generar_dataset_enriquecido.py --n 10000000 --seed 42`.

**Principio:** las columnas nuevas se derivan de los datos reales con reglas deterministas.
Solo la suciedad (duplicados, filas corruptas, prefijos/typos de busqueda) es aleatoria con semilla fija.

| Metrica | Valor |
|---|---|
| Filas originales procesadas | 10,000,000 |
| Filas de salida | 10,099,764 |
| Duplicados inyectados | 99,764 |
| Filas corruptas inyectadas | 9,805 |
| Ordenes | 126,641: express 79,221 (62.6%) / standard 40,028 (31.6%) / scheduled 7,392 (5.8%) |
| Usuarios sospechosos | 973 (compras_por_hora 726, eventos_por_minuto 247) |
| Filas de usuarios sospechosos | 71,186 |
| Cuentas en blacklist historica | 307 |

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
| order_type | 2 | Derivada | `scheduled` si el primer producto comprado es voluminoso (categoria en furniture, construction, country_yard, appliances.kitchen.refrigerators, appliances.kitchen.washer, appliances.kitchen.oven, appliances.kitchen.dishwasher, appliances.kitchen.hood, appliances.environment.air_conditioner); `standard` si la sesion paso por carrito antes de comprar; `express` si es compra directa sin carrito ("comprar ahora") |
| scheduled_time | 2 | Derivada | Solo `scheduled`: fecha de compra + 2 dias, 09:00 UTC |
| stock | 3 | Derivada | Stock inicial = ceil(compras reales del producto en el periodo x 1.10), minimo 1; baja 1 por compra |
| search_query | 9 | Derivada + ruido | 10% de las vistas: marca o ultima parte de la categoria del producto visto; 30% solo prefijo, 10% con transposicion de letras |
| is_suspicious | eval. 6 | Ground truth derivado | 1 si la cuenta tuvo >= 5 compras en una misma hora o >= 10 eventos en un mismo minuto (conteo exacto, ventanas fijas) en algun momento del periodo |
| is_duplicate | eval. 1 | Ruido inyectado | 1 si es un duplicado inyectado (1%, aparece 1-50 filas despues, mismo contenido, nuevo event_id) |
| is_invalid | eval. 1 | Ruido inyectado | 1 si la fila fue corrompida (0.1%: precio negativo, user_id vacio, timestamp invalido o event_type mal formado) |

**Importante:** `is_suspicious`, `is_duplicate` e `is_invalid` son etiquetas de evaluacion. El pipeline NO debe
leerlas para decidir; solo se usan para medir aciertos, falsos positivos y falsos negativos de las etapas 1 y 6.

Nota: el dataset original ya trae duplicados exactos naturales (~0.04%), no marcados en `is_duplicate`.

## blacklist.csv

Cuentas (`user_id`) cuya primera ventana anomala ocurrio antes de 2019-11-03 (historico de cuentas ya
senaladas). Se carga en el Bloom Filter. Las cuentas sospechosas que aparecen despues no estan en la lista y
debe detectarlas el Count-Min Sketch por intensidad de actividad en ventana. Un positivo del Bloom se verifica
contra la lista exacta antes de bloquear.
