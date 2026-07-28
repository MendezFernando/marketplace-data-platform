# ADR-0001 — Arquitectura de almacenamiento y procesamiento de la plataforma

- **Estado:** ✅ Aceptado
- **Fecha:** 2026-07-27
- **Decisores:** Fernando Méndez
- **Relacionado con:** [`docs/00-business-requirements.md`](../00-business-requirements.md)

---

## Contexto y planteamiento del problema

La plataforma debe integrar cuatro fuentes de naturaleza muy distinta:

| Fuente | Formato | Volumen | Cadencia |
|---|---|---|---|
| Catálogo transaccional del marketplace | CSV estructurado, 9 tablas relacionadas | ~1.5 M filas (histórico completo) | Carga inicial + incrementales diarios |
| API de tipo de cambio BRL→USD | JSON | ~1 fila/día | Diaria, con huecos en fines de semana y feriados |
| API de clima histórico | JSON anidado | ~10 K filas/día | Diaria, por región |
| Clickstream de navegación | Eventos JSON | Millones de eventos/día | Continua |

De los requisitos de negocio se derivan las siguientes fuerzas técnicas:

- **BQ-04 exige conservar el historial de atributos que la fuente sobrescribe.** El segmento de un
  vendedor debe poder reconstruirse tal como era en cualquier fecha pasada. Esto requiere
  actualizaciones y *upserts* sobre datos ya almacenados, no solo escrituras nuevas.
- **BQ-05 exige el grano de evento con latencia inferior a 5 minutos** sobre el único conjunto de
  datos con volumen real de la plataforma.
- **BQ-01 exige reproducibilidad histórica:** un mismo cálculo ejecutado en dos fechas distintas
  debe devolver el mismo resultado, lo que obliga a conservar el estado de los datos de entrada.
- **BQ-01 y BQ-03 dependen de APIs externas** cuyo esquema puede cambiar sin aviso y cuyo histórico
  es limitado o está sujeto a límites de peticiones: **reprocesar no puede implicar volver a
  extraer de la fuente**.
- **SLA-02 (completitud) exige reconciliar conteos** entre lo recibido y lo publicado, lo que
  requiere conservar lo recibido en su forma original.
- **SLA-03 (calidad) exige detener la publicación ante datos corruptos**, lo que requiere que una
  ejecución fallida no deje datos parcialmente visibles a los consumidores.

Restricciones del entorno:

- Ejecución **local** en una máquina con 16 GB de RAM y 16 vCPU.
- **Coste cero**: solo herramientas open source o de capa gratuita.
- La plataforma debe poder **migrarse a la nube** (Azure) sin reescribir la lógica de procesamiento.
- Objetivo de portafolio: la arquitectura debe reflejar herramientas y patrones **efectivamente
  usados en empresas**, evitando dependencia de proveedores propietarios.

**Pregunta a responder:** ¿cómo debe almacenarse y organizarse el dato desde la ingesta hasta el
consumo?

---

## Opciones consideradas

1. **Data Warehouse tradicional** — cargar todo directamente a PostgreSQL modelado.
2. **Data Lake clásico** — archivos Parquet en MinIO sin formato de tabla transaccional.
3. **Lakehouse con arquitectura de medallón** — MinIO + Delta Lake (Bronze/Silver/Gold) + PostgreSQL como capa de servicio.

---

## Decisión

Adoptamos la **opción 3**: un *lakehouse* sobre almacenamiento de objetos compatible con S3
(MinIO), con **Delta Lake** como formato de tabla, organizado según la **arquitectura de medallón**
(Bronze / Silver / Gold), y **PostgreSQL como capa de servicio** para los modelos Gold que
alimentan dashboards y API.

---

## Justificación

**1. El almacenamiento de objetos desacopla el coste del volumen y habilita conservar el crudo.**
Bronze puede guardar el 100 % de lo recibido de forma indefinida a coste marginal. Esto es lo que
hace posible reprocesar sin volver a extraer de las APIs externas —restricción impuesta por BQ-01 y
BQ-03— y lo que da la copia de referencia que SLA-02 necesita para reconciliar conteos.

**2. Delta Lake aporta `MERGE`, que es el requisito duro de BQ-04.** Mantener una dimensión de
vendedores con SCD Tipo 2 significa cerrar el registro vigente e insertar el nuevo en la misma
operación. Sin soporte de actualización sobre el lake, BQ-04 sería irresoluble sin reescribir la
tabla completa en cada ejecución.

**3. Las garantías ACID de Delta Lake hacen viable el *circuit breaker* de SLA-03.** Una ejecución
que falla a mitad de escritura nunca llega a publicarse en el log de transacciones, por lo que los
consumidores siguen viendo la última versión válida en lugar de datos parciales.

**4. El *time travel* soporta la reproducibilidad exigida por BQ-01 y la auditoría de incidentes.**
Permite responder «¿por qué el reporte de ayer daba otro número?» comparando versiones, en lugar de
reconstruir el estado por deducción.

**5. El *schema enforcement* protege frente a cambios no anunciados en las APIs externas**, que es
el modo de fallo más probable de BQ-01 y BQ-03 y una de las cuatro dimensiones de calidad que la
plataforma se compromete a vigilar.

**6. El formato columnar (Parquet, base de Delta) se ajusta al patrón de acceso analítico.**
BQ-01, BQ-02 y BQ-04 son agregaciones sobre pocas columnas de tablas largas: leer solo las columnas
necesarias reduce el I/O en un orden de magnitud frente a un formato por filas.

**7. PostgreSQL como capa de servicio cubre el punto débil del lakehouse.** Un lakehouse es
eficiente en escaneos analíticos pero pobre en consultas interactivas pequeñas y concurrentes, que
es exactamente el patrón de Power BI y de la API. PostgreSQL entrega SQL estándar con conectores
maduros y baja latencia por consulta. Los volúmenes de Gold son agregados y caben holgadamente.

**8. Todo el stack se basa en formatos y protocolos abiertos.** MinIO expone la API de S3 y Delta
almacena Parquet: migrar a Azure Data Lake Storage implica cambiar el endpoint y las credenciales,
no reescribir la lógica de transformación. Esto satisface la restricción de portabilidad a la nube.

**9. El medallón fija un contrato explícito sobre el estado del dato.** Cada capa declara qué
garantías ofrece —Bronze: fidelidad; Silver: corrección; Gold: significado de negocio— lo que
delimita responsabilidades y evita que la plataforma degenere en un conjunto de archivos sin
procedencia conocida.

### Por qué se descartaron las alternativas

| Opción | Motivo del descarte |
|---|---|
| **Data Warehouse tradicional** | El *schema-on-write* obliga a definir y mantener el esquema antes de ingerir, lo que fricciona con APIs externas cambiantes (BQ-01, BQ-03) y con JSON anidado (clima). No ofrece un lugar económico para conservar el crudo, por lo que reprocesar exigiría volver a extraer de fuentes con histórico limitado. El volumen y la cadencia del clickstream (BQ-05) no encajan en un PostgreSQL de instancia única. Además acopla almacenamiento y cómputo, lo que impide escalarlos por separado. |
| **Data Lake clásico** | Sin transacciones ACID, una ejecución fallida deja archivos parciales visibles y hace inviable el *circuit breaker* de SLA-03 y la reconciliación de SLA-02. Sin `UPDATE`/`DELETE` no es posible implementar SCD Tipo 2 (BQ-04) ni atender solicitudes de borrado por regulación. Sin *schema enforcement* nada impide escribir datos incompatibles, que es el camino documentado hacia un *data swamp*. Las escrituras concurrentes carecen de aislamiento. |

---

## Consecuencias

### Positivas

- **Reprocesabilidad total.** Silver y Gold son artefactos derivados: ante un error de lógica se
  reconstruyen desde Bronze sin depender de las fuentes originales.
- **Reproducibilidad histórica.** El *time travel* y la conservación del crudo permiten que un
  cálculo sobre un período pasado devuelva siempre el mismo resultado.
- **Portabilidad a la nube sin reescritura**, al depender solo de la API de S3 y de formatos abiertos.
- **Separación clara de responsabilidades** por capa, con criterios de calidad distintos en cada una.
- **Coste de almacenamiento marginal**, lo que permite políticas de retención generosas.

### Negativas / costos asumidos

- **Complejidad operativa alta.** La plataforma requiere coordinar MinIO, Spark (JVM), PostgreSQL y
  Airflow simultáneamente sobre 16 GB de RAM. Es sustancialmente más difícil de operar y depurar que
  una única base de datos.
- **Sobredimensionamiento respecto al volumen real.** Salvo el clickstream, todas las fuentes caben
  en memoria; DuckDB o pandas resolverían Bronze→Silver en segundos y sin infraestructura
  distribuida. Se asume el sobrecoste de forma deliberada por dos razones: BQ-05 sí exige
  procesamiento distribuido, y el objetivo de portafolio requiere demostrar competencia en el stack
  que las empresas usan a escala. **No es la elección que se haría en una empresa cuyo volumen se
  mantuviera en este orden de magnitud.**
- **Triplicación del almacenamiento.** El mismo dato reside en Bronze, Silver y Gold. Es aceptable
  dado el coste del almacenamiento de objetos, pero deja de serlo a escala de petabytes sin
  políticas de retención.
- **Ciclo de desarrollo más lento.** Iniciar una sesión de Spark cuesta decenas de segundos, lo que
  penaliza la iteración local frente a ejecutar Python directamente.
- **Dos motores de transformación.** Spark (Bronze→Silver) y dbt (Silver→Gold) implican dos
  lenguajes, dos suites de tests y dos modelos mentales. Se acepta porque cada uno es claramente
  superior en su ámbito, pero incrementa la superficie de mantenimiento.

### Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Spark agota la memoria de la máquina y bloquea el entorno | Limitar `spark.driver.memory` explícitamente y agrupar los servicios en perfiles de Docker Compose para no levantar todo a la vez |
| Bronze degenera en un *data swamp* sin procedencia conocida | Convención de rutas obligatoria, columnas de metadatos (`_ingested_at`, `_source`, `_batch_id`) en toda escritura y catálogo de datos en el Módulo 8 |
| Delta Lake pierde tracción frente a Apache Iceberg | Los datos base son Parquet estándar y los conceptos (log de transacciones, `MERGE`, *time travel*) son equivalentes; una migración sería mecánica, no conceptual |
| Cambios de esquema no anunciados en las APIs externas rompen la ingesta | Validación con contratos Pydantic en la ingesta y *schema enforcement* en la escritura a Silver; fallo explícito y ruidoso en lugar de corrupción silenciosa |
| La complejidad del stack impide terminar el proyecto | Incorporación incremental por módulos; cada módulo deja la plataforma en estado funcional |

---

## Notas de implementación

| Capa | Ubicación | Formato |
|---|---|---|
| **Bronze** | `s3a://bronze/{fuente}/{tabla}/ingestion_date=YYYY-MM-DD/` | **Parquet**, particionado por fecha de ingesta, *append-only* |
| **Silver** | `s3a://silver/{entidad}/` | **Delta Lake**, particionado según patrón de consulta |
| **Gold** | `s3a://gold/{mart}/` y esquema `gold` en PostgreSQL | **Delta Lake** + tablas materializadas en PostgreSQL |

**Sobre el formato de Bronze:** se usa Parquet plano y no Delta porque Bronze es *append-only* por
definición y no necesita `MERGE`, actualizaciones ni resolución de concurrencia. Añadir un log de
transacciones aportaría complejidad sin resolver ningún requisito de esa capa. Los *payloads* JSON
originales de las APIs se conservan además sin transformar, para poder auditar exactamente qué
devolvió la fuente. Es una decisión revisable: muchos equipos usan Delta en las tres capas por
uniformidad operativa.

**Sobre el particionamiento:** Bronze se particiona por fecha de ingesta (cuándo lo recibimos) y no
por fecha de negocio (cuándo ocurrió), porque la fecha de ingesta es la única conocida con certeza
en el momento de escribir y es la que permite reprocesar un lote concreto. Silver y Gold se
particionan por fecha de negocio, que es como se consultan.
