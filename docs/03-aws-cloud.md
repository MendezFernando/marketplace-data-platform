# Módulo 7.5 — El lakehouse en AWS

Migración de la plataforma de local (MinIO) a AWS, y publicación del modelo
dimensional como tablas consultables con SQL sin servidores.

---

## 1. Qué se construyó

```
data/landing/*.csv
      │  ingesta idempotente, partición por fecha lógica
      ▼
┌──────────────────── Amazon S3 (Delta Lake) ────────────────────┐
│  mdp-fmendez-bronze  →  mdp-fmendez-silver  →  mdp-fmendez-gold │
│     crudo, versionado      limpio y tipado      modelo estrella  │
└──────────┬──────────────────────────────────────────┬───────────┘
           │                                          │
           ▼                                          ▼
   AWS Glue Data Catalog                       PostgreSQL (gold)
   9 tablas registradas como Delta                     │
           │                                           ▼
           ▼                                   dbt · 4 marts · 33 tests
      Amazon Athena                                    │
   SQL ad hoc, sin servidores                          ▼
                                                 Power BI / API

Orquestación:  Airflow — 14 tareas, reintentos, backfill, depends_on_past en el SCD2
Seguridad:     IAM con privilegio mínimo · MFA en la raíz · presupuesto con alerta
```

## 2. Decisiones y por qué

| Decisión | Motivo |
|---|---|
| Código desacoplado del proveedor | El proyecto hablaba con la **API de S3**, no con MinIO. Migrar fue cambiar variables de entorno; **cero líneas de Python**. |
| `S3_ENDPOINT` vacío = AWS | Un solo interruptor decide el entorno. Con endpoint: MinIO local. Sin endpoint: S3 real. |
| `path.style.access` distinto por entorno | MinIO exige rutas `host/bucket`; AWS usa subdominios. Confundirlo produce errores difíciles de diagnosticar. |
| Tablas registradas como **Delta**, no Parquet | Un catálogo que lee "todos los parquet de la carpeta" cuenta filas de versiones antiguas. Registrar el tipo Delta hace que se respete el `_delta_log`. |
| Crawler **on demand**, no programado | Cobra por tiempo de ejecución. El esquema cambia rara vez; los datos, todos los días. |
| Rol para el crawler, llaves solo en local | Lo que corre **dentro** de AWS usa roles con credenciales temporales. Las llaves estáticas son solo para la laptop. |
| Privilegio mínimo en el usuario | Se retiró `AdministratorAccess` y se dejó una política a la medida. Verificado: el trabajo diario funciona y todo lo demás está denegado. |

## 3. Cifras reales

| Métrica | Valor |
|---|---|
| Filas en Silver | 566,358 en 8 tablas |
| Tablas en Gold | 9 (5 dimensiones, 4 hechos) |
| Versiones en `dim_seller` (carga inicial) | 3,096 |
| Tablas catalogadas en Glue | 9, todas `table_type=delta` |
| Pruebas de dbt | 33 (29 genéricas + 4 singulares) |
| Tareas del DAG diario | 14, ~12 min de extremo a extremo |
| Costo del crawler por corrida | 0.253 DPU-hora |
| Latencia local vs S3 (misma tarea) | 16 s → 58 s |

La diferencia de latencia no es un defecto de S3: es la red. En producción el
cómputo vive **dentro** de AWS (EMR, Glue), junto a los datos. De ahí el
principio: *mueve el cómputo hacia los datos, no los datos hacia el cómputo*.

## 4. Verificación de los permisos mínimos

```
Debe FUNCIONAR (trabajo diario)          Debe ESTAR BLOQUEADO
  PERMITIDO  leer el lakehouse             DENEGADO  listar servidores EC2
  PERMITIDO  escribir en el lakehouse      DENEGADO  borrar un bucket completo
  PERMITIDO  ver el catálogo de Glue       DENEGADO  crear otros usuarios
  PERMITIDO  consultar con Athena
```

Después de retirar `AdministratorAccess`, el pipeline de Spark siguió escribiendo
en S3 sin cambios: la política cubre exactamente lo necesario, ni más ni menos.

## 5. Control de costo

- Presupuesto de $1 USD con alerta al 80% y al 100%.
- Resultados de Athena con expiración automática a los 7 días.
- Buckets privados, sin acceso público.
- Athena cobra por **datos escaneados**: elegir columnas y particionar es lo que
  hace barata una consulta; `LIMIT` no reduce el costo.

## 6. Equivalencias en otras nubes

| Concepto | AWS | Azure | GCP |
|---|---|---|---|
| Almacén de objetos | S3 | ADLS Gen2 | Cloud Storage |
| Catálogo | Glue Data Catalog | Unity Catalog / Purview | Dataplex |
| SQL sobre archivos | Athena | Synapse Serverless | BigQuery |
| Spark administrado | EMR / Glue | Databricks | Dataproc |
| Airflow administrado | MWAA | Managed Airflow | Cloud Composer |

Cambia el conector y el nombre; no cambian el diseño ni los principios:
particiones, idempotencia, catálogo y costo por datos escaneados.
