# Music Data Reliability

**Español** · [English](README.md)

Prototipo de **Data Reliability** y **Data Observability** sobre un sistema de análisis de preferencias musicales.

Cuatro capas de control independientes —contrato de entrada, transformación, SLOs y observabilidad— protegen un dataset de features destinado a alimentar un motor de recomendación. El proyecto no afirma que las cuatro sean necesarias: lo mide.

## La tesis

Un sistema de preferencias musicales infiere el gusto del comportamiento, sin calificaciones explícitas contra las cuales contrastar. Un evento corrupto no es un registro perdido: es una opinión falsificada atribuida a una persona real.

Las fallas se dividen en dos familias con naturalezas opuestas:

- Un `ms_played` mayor que la duración de la canción es **imposible** y se puede escribir como regla.
- Un `ms_played` inflado un 25% es **plausible en cada fila** y solo se ve comparando la distribución contra su historia.

Las dos rompen el producto por igual, y ningún tipo de control detecta ambas. La demostración es la matriz de detección: 14 anomalías contra 4 controles, medida y no declarada.

## Separación de responsabilidades

| Control | Pregunta que responde | Capa | Ante falla |
| --- | --- | --- | --- |
| Great Expectations | ¿El dato entrante cumple el contrato? | Pre-`RAW` | Bloquea o pone en cuarentena |
| dbt tests | ¿El modelo es estructuralmente coherente? | `STAGING`, `CORE` | Falla el build |
| Soda | ¿El dataset cumple su SLO? | `CORE`, `MARTS` | Alerta y retira la certificación |
| Monte Carlo | ¿El dato se comporta como siempre? | Todas | Alerta con lineage |

Reglas de no-duplicación, aplicadas en el código:

1. Un chequeo se declara una sola vez, en la capa donde primero es exigible.
2. Great Expectations no valida tablas transformadas; su dominio termina en `RAW`.
3. Soda no reimplementa tests estructurales de dbt.
4. **Monte Carlo no recibe umbrales.** En el momento en que se le escribe uno, deja de ser observabilidad y pasa a ser un check de Soda con otro nombre.

## Dos decisiones de diseño que sostienen todo

**Nada se descarta en silencio.** `stg_listening_events` deduplica, pero marca cada fila con `was_deduplicated`. `fct_listening_events` usa `LEFT JOIN` contra el catálogo y marca `is_orphan_song` en vez de filtrar. `stg_rejected_events` reúne todo lo descartado con su motivo. Un test que pasa porque las filas ofensivas fueron eliminadas antes es peor que no tener test: reporta una salud fabricada por el propio pipeline.

**La certificación la decide Soda, no dbt.** `feature_eligible` es una propiedad del usuario (tiene señal suficiente para perfilarlo). `MARTS.DATA_QUALITY_STATUS.CERTIFIED` es una propiedad del dataset y la escribe `certify.py` leyendo el resultado del scan. Un dataset que reprueba sus checks no puede certificarse a sí mismo.

## Requisitos

| Requisito | Versión | Notas |
| --- | --- | --- |
| Python | 3.11 | El pin de `numpy<2` lo impone Great Expectations 0.18 |
| Docker + Compose | 24.0 / v2 | Kafka, Redis, LocalStack |
| Cuenta Snowflake | Trial alcanza | Único servicio remoto obligatorio |
| Cuenta Monte Carlo | — | Opcional: sin ella el pipeline corre con tres capas |

## Puesta en marcha

```bash
cp .env.example .env && $EDITOR .env
python -m venv .venv && source .venv/bin/activate
make setup
make snowflake-init
make pipeline
```

`make help` lista todos los targets.

## El experimento

```bash
make baseline                 # 14 días de historia limpia
make experiment ANOMALY=A9    # inyecta, corre el pipeline y mide
make experiment-all           # las 14
make matrix                   # construye la matriz observada
```

`run_injection.py` inyecta una anomalía, ejecuta el pipeline completo y escribe una fila por cada par (anomalía × control) con lo observado y la latencia de detección. `build_matrix.py` solo lee esas corridas. Si no hay ninguna, falla:

```
No runs found in experiment/results/.
The matrix is measured, not declared: run `make experiment` first.
```

La hipótesis vive aparte, en `experiment/expected_matrix.yml`, y el reporte contrasta lo observado contra ella. Una expectativa que resulta falsa es un hallazgo para escribir, no un número para corregir en silencio.

Se registran tres estados, no dos: **detectó**, **no detectó** y **control no disponible**. Una herramienta ausente y una herramienta que no se dio cuenta son hallazgos distintos, y colapsarlos invalida la matriz. Cuando el contrato rechaza el lote, dbt y Soda quedan como no disponibles: nunca llegaron a ver la anomalía.

### Catálogo de anomalías

| # | Anomalía | Familia |
| --- | --- | --- |
| A1 | `user_id` nulo en 3% | Determinística |
| A2 | `ms_played` mayor que `duration_ms` | Determinística |
| A3 | `event_type` desconocido (`share`) | Determinística |
| A4 | `event_id` duplicado en 1% | Determinística |
| A5 | `song_id` fuera del catálogo | Estructural |
| A6 | Columna `country` eliminada | Estructural |
| A7 | Retraso de carga de 10 h | Temporal |
| A8 | Caída del 40% de eventos de `BR` | Comportamental |
| A9 | Skip rate de 32% a 19% | Comportamental |
| A10 | Volumen duplicado | Comportamental |
| A11 | `mobile` reetiquetado como otras plataformas | Comportamental |
| A12 | `ms_played` desplazado +25% | Comportamental |
| A13 | 15% de usuarios reducidos a un evento | Cobertura |
| A14 | `duration_ms` de INTEGER a STRING | Estructural |

A11 **reetiqueta** en lugar de borrar. Eliminando las filas se perdería el 57% del volumen y cualquier monitor de volumen lo vería; el punto de A11 es una anomalía visible únicamente en la distribución de un campo.

A7 retrasa `event_ts` **y** `_ingested_at`. Mover solo el primero dejaría ciego a `dbt source freshness`, que es justamente el control que debe atraparlo.

## Modelo de datos

El contrato está en [`ingestion/great_expectations/contract.yml`](ingestion/great_expectations/contract.yml). Ese archivo **es** el contrato: `validate_batch.py` no contiene reglas propias, solo ejecuta lo declarado ahí. Cambiarlo es un pull request revisable.

Invariantes del dominio:

- `ms_played <= duration_ms`
- Un `skip` implica `ms_played < duration_ms * 0.8`
- Un `like` puede tener `ms_played` en 0
- Un `song_id` mapea siempre al mismo `artist_id` y al mismo `genre`
- Los eventos de una `session_id` pertenecen a un solo `user_id`

Política de fallo: violaciones de fila van a cuarentena con su motivo; si superan el 5% del lote, se rechaza el lote entero. Una falla de esquema rechaza el lote sin importar el porcentaje, porque el esquema no es una propiedad por fila.

### Capas

| Esquema | Contenido | Garantía |
| --- | --- | --- |
| `RAW` | Eventos que pasaron el contrato, con `_batch_id` | Inmutable |
| `RAW.LISTENING_EVENTS_QUARANTINE` | Rechazados, con motivo | Auditable |
| `STAGING` | Tipado, deduplicado y marcado | Un registro por evento |
| `CORE` | `dim_*` y `fct_listening_events` | Integridad, huérfanos marcados |
| `MARTS` | Features y afinidades, ventana de 30 días | Certificable |

Salida principal: `MARTS.USER_LISTENING_FEATURES`, una fila por usuario con `top_genre`, `genre_diversity` (entropía de Shannon normalizada), `skip_rate`, `replay_rate`, `like_rate`, `sessions_30d`, `avg_session_minutes`, `activity_tier` y `feature_eligible`.

## Generador

Vectorizado: 100.000 eventos en menos de un segundo.

- **Estacionalidad horaria y semanal.** Monte Carlo necesita una forma que aprender; timestamps uniformes no le enseñan nada.
- **Sesiones reales**: eventos contiguos del mismo usuario con menos de 30 minutos de separación. Sin esto, cualquier feature de sesión es ruido.
- **Reproducible**: con `--seed` y `--as-of` fijos, dos corridas producen frames idénticos. Sin `--as-of` el output depende del reloj y el experimento no es reproducible.

```bash
make generate DAYS=7 AS_OF=2026-09-16T00:00:00
```

## Tests

```bash
make verify       # todo lo verificable sin warehouse: lint + 58 tests
make test         # suite de pytest
make test-models  # el SQL real de dbt sobre DuckDB
make lint         # dbt parse + gramática Snowflake vía sqlfluff
```

Snowflake es el único servicio remoto y no hace falta para validar casi nada. Tres capas de verificación cubren lo que se puede cubrir sin él:

| Capa | Qué prueba | Qué no prueba |
| --- | --- | --- |
| `dbt parse` | Refs, Jinja, YAML, grafo de dependencias | No ejecuta ninguna consulta |
| `sqlfluff` | Que el SQL parsee contra la gramática de Snowflake | No valida semántica ni tipos |
| DuckDB | El SQL real de los modelos sobre datos generados | DuckDB no es Snowflake |

`sqlfluff` está configurado como puerta de sintaxis, no de estilo: la lista de reglas se reduce a una trivial, porque los errores de parseo y de plantilla se reportan igual. Un linter discutiendo espacios entrena a todo el mundo a ignorar la única señal que importa.

La suite cubre cuatro cosas: que el generador sea reproducible y respete las invariantes, que **cada anomalía haga lo que su nombre afirma**, que el contrato acepte lo limpio y rechace exactamente lo prohibido, y que los modelos produzcan la aritmética correcta.

El tercer grupo incluye la prueba que da sentido al proyecto:

```python
@pytest.mark.parametrize("anomaly", ["A5","A7","A8","A9","A10","A11","A12","A13"])
def test_behavioural_anomalies_pass_the_contract(...):
    assert res["verdict"] == "accepted"
```

Ocho anomalías **deben** atravesar el control determinístico sin una sola falla. Si alguna fuera atrapada ahí, el argumento a favor de una capa de comportamiento se debilitaría.

Sin estos tests, un falso negativo en la matriz podría ser un bug del inyector en lugar de un punto ciego del control, y no habría forma de distinguirlo.

`test_model_semantics.py` ejecuta el SQL **sin modificar** de los once modelos contra datos generados, con tres funciones adaptadas (`iff`, `dateadd`, `current_timestamp()`) porque DuckDB las escribe distinto. Verifica lo que ninguna de las otras capas puede: que la entropía quede en [0,1], que las afinidades por género sumen 1, que la normalización min-max sea por usuario, y que un `song_id` huérfano llegue marcado a la tabla de hechos en vez de desaparecer.

Esa última prueba falla si alguien revierte el `LEFT JOIN` a un join interno. Es la regresión más peligrosa del proyecto y ahora tiene un test que la atrapa.

## Estructura

```
generator/          catálogo, eventos y las 14 anomalías
ingestion/          contrato YAML, gate, carga idempotente, certificación
dbt_music/          staging → core → marts, tests y contratos
soda/               checks de raw, core y marts
montecarlo/         monitores como código, sin umbrales
experiment/         arnés de medición e hipótesis
infra/snowflake/    bootstrap idempotente
tests/              pytest
docs/               contrato y matriz observada
```

## Estado

Implementado y verificado localmente: generador, anomalías, contrato de ingesta con cuarentena, arnés de medición, suite de tests.

Requiere Snowflake solo para la ejecución real: `dbt build` contra el warehouse, los scans de Soda y la certificación. La lógica de los modelos y su sintaxis ya están verificadas sin él. Requiere cuenta de Monte Carlo: la cuarta columna de la matriz.

Fuera de alcance en esta fase: entrenamiento de modelos, API de serving, streaming en producción y operación multi-región.
