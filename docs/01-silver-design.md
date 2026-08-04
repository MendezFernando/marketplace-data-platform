# Diseño de la capa Silver

> **Estado:** ✅ v1 — aprobado para implementación
> **Autor:** Fernando Méndez
> **Relacionado con:** [`00-business-requirements.md`](00-business-requirements.md) · [`adr/0001-arquitectura-lakehouse-medallon.md`](adr/0001-arquitectura-lakehouse-medallon.md)

---

## 1. Objetivo

La capa Silver representa **la verdad operacional del negocio**. Aquí los datos dejan de ser una
copia fiel del sistema origen y se convierten en entidades limpias, tipadas, consistentes y
reutilizables por cualquier caso de uso analítico.

Su propósito es construir un modelo estable e **independiente** de los dashboards o modelos
dimensionales que después consumirán la capa Gold.

| En Silver **sí** se hace | En Silver **no** se hace |
|---|---|
| Conversión de tipos de datos | Agregaciones |
| Estandarización de formatos | Métricas de negocio |
| Eliminación de duplicados | Modelado dimensional (`dim_` / `fct_`) |
| Validación de reglas de calidad | Tablas anchas orientadas a un reporte |
| Resolución de identidad y de conflictos | |
| Integración de tablas que son el mismo concepto | |
| Conservación de la granularidad atómica | |

---

## 2. Principios de diseño

1. **Cada entidad representa un único concepto de negocio.**
2. **La granularidad es homogénea dentro de cada entidad**, y se declara explícitamente.
3. **Entidades de distinto grano no se fusionan.** Se mantienen como hechos conformados que
   comparten claves, evitando *fan traps*.
4. **La información permanece lo más cercana posible al dato original**; Silver limpia, no interpreta.
5. **Toda clave primaria fue verificada con datos**, no asumida.
6. **El diseño favorece la reutilización** por casos de uso aún no definidos.

---

## 3. Entidades de Silver

### 3.1 `customers`

| | |
|---|---|
| **Origen** | `olist_customers_dataset` |
| **Grano** | Una fila por **persona** (`customer_unique_id`) |
| **Clave primaria** | `customer_unique_id` |
| **Volumen esperado** | ~96 096 filas (desde 99 441 en origen) |

**Justificación del grano.** El sistema origen genera un `customer_id` distinto **por cada orden**,
por lo que no identifica a una persona sino a su aparición en un pedido. Verificado:

```
filas en origen             : 99 441
customer_id distintos       : 99 441   ← una fila por fila: no aporta identidad
customer_unique_id distintos: 96 096   ← personas reales
personas con más de 1 compra:  2 997
```

Usar `customer_id` como clave haría que **cada cliente comprara exactamente una vez**, y la tasa de
recompra de **BQ-02** saldría 0% — un número plausible y completamente falso.

**Resolución de conflictos de atributos.** Una misma persona puede aparecer con distintos valores de
ciudad o estado entre sus pedidos:

```
personas con más de una ciudad : 122
personas con más de un estado  :  39
```

> **Decisión: se conserva el valor asociado a la orden más reciente** (`max(order_purchase_timestamp)`).
> **Motivo:** los atributos de ubicación describen *dónde está el cliente hoy*, que es lo relevante
> para segmentación y logística. Alternativas descartadas: el valor más frecuente (más robusto ante
> errores de captura, pero desactualizado si la persona se mudó de verdad) y el primer valor
> (congela información obsoleta).
>
> **Limitación aceptada:** esta estrategia **no conserva el historial** de ubicaciones. Si en el
> futuro un requisito exige atribuir ventas pasadas a la ubicación vigente en aquel momento, esta
> entidad deberá convertirse en una dimensión SCD Tipo 2.

**SCD Tipo 2 (evolución futura).** El dataset actual no incluye columnas históricas
(`valid_from`, `valid_to`, `is_current`). Implementarlo requeriría generar una **clave sustituta
propia del pipeline** (`customer_sk`), **distinta de `customer_unique_id`**: al versionar, la clave
natural deja de ser única y ya no sirve para que un hecho apunte a la versión correcta.

| Columna | Tipo |
|---|---|
| `customer_unique_id` | string |
| `customer_zip_code_prefix` | string |
| `customer_city` | string |
| `customer_state` | string |

**Limpieza:** ZIP conservado como texto (preserva ceros iniciales) · estandarización de nombres de
ciudad · normalización de códigos de estado a mayúsculas · validación de unicidad y de nulos en la
clave.

---

### 3.2 `orders`

| | |
|---|---|
| **Origen** | `olist_orders_dataset` |
| **Grano** | Una fila por **orden** |
| **Clave primaria** | `order_id` (verificada: 99 441 filas, 0 duplicados, 0 nulos) |

**Justificación.** `orders` se mantiene como entidad propia, **separada de `order_items`**, porque
sus granos son distintos: una orden tiene una fila y N ítems. Fusionarlas tendría dos consecuencias:

1. **Se perderían 775 órdenes sin ítems.** Verificado:

   ```
   órdenes totales   : 99 441
   órdenes con ítems : 98 666
   órdenes sin ítems :    775   →  unavailable 603 · canceled 164 · created 5 · invoiced 2 · shipped 1
   ```

   Son precisamente las órdenes necesarias para analizar cancelaciones y falta de disponibilidad, y
   su desaparición incumpliría el **SLA-02** (completitud) desde la primera ejecución.

2. **Los atributos de orden se repetirían por ítem**, de modo que cualquier métrica a nivel orden
   exigiría `COUNT(DISTINCT order_id)`; olvidarlo inflaría el resultado — el mismo *fan trap* que se
   evita separando `payments`.

**Enlace con `customers`.** Esta entidad materializa la resolución de identidad: conserva
`customer_id` (clave del origen, para trazabilidad) y **añade `customer_unique_id`** resuelto desde
`olist_customers_dataset`. Sin esta columna, `orders` y `customers` no tendrían ninguna clave en
común y **BQ-02 sería irresoluble**. Verificado que la relación es limpia: **cero** `customer_id`
apuntando a más de un `customer_unique_id`.

| Columna | Tipo |
|---|---|
| `order_id` | string |
| `customer_id` | string |
| `customer_unique_id` | string ← *resuelto en Silver* |
| `order_status` | string |
| `order_purchase_timestamp` | timestamp |
| `order_approved_at` | timestamp |
| `order_delivered_carrier_date` | timestamp |
| `order_delivered_customer_date` | timestamp |
| `order_estimated_delivery_date` | timestamp |

**Limpieza:** conversión de las 5 columnas de fecha de texto a `timestamp` en UTC · normalización de
`order_status` · validación de coherencia temporal (compra ≤ aprobación ≤ envío ≤ entrega).

---

### 3.3 `order_items`

| | |
|---|---|
| **Origen** | `olist_order_items_dataset` |
| **Grano** | Una fila por **producto vendido dentro de una orden** |
| **Clave primaria** | `(order_id, order_item_id)` (verificada: 112 650 filas, 0 duplicados) |

**Justificación.** Es el grano atómico de la venta y el nivel donde `price` y `freight_value` son
aditivos. Se une a `orders` por `order_id` cuando un análisis requiere contexto de la orden, pero
**no se fusiona**: son hechos conformados que comparten dimensiones.

| Columna | Tipo |
|---|---|
| `order_id` | string |
| `order_item_id` | integer |
| `product_id` | string |
| `seller_id` | string |
| `shipping_limit_date` | timestamp |
| `price` | decimal |
| `freight_value` | decimal |

**Limpieza:** conversión de importes a decimal (**no** a float, para evitar errores de redondeo en
cálculos monetarios) · validación de integridad referencial contra `orders`, `products` y `sellers`
· verificación de importes no negativos.

---

### 3.4 `payments`

| | |
|---|---|
| **Origen** | `olist_order_payments_dataset` |
| **Grano** | Una fila por **pago realizado sobre una orden** |
| **Clave primaria** | `(order_id, payment_sequential)` (verificada: 103 886 filas, 0 duplicados) |

**Justificación.** Permanece **independiente**. Una orden puede tener varios pagos (por ejemplo,
tarjeta más *voucher*), de modo que unirla con `order_items` produciría un producto cartesiano:
una orden de 3 ítems con 2 pagos generaría 6 filas y **duplicaría los ingresos**. Esta separación
evita un *fan trap*; el cruce entre ambos hechos se resuelve agregando cada uno a un grano común
antes de unirlos (*drill-across*).

| Columna | Tipo |
|---|---|
| `order_id` | string |
| `payment_sequential` | integer |
| `payment_type` | string |
| `payment_installments` | integer |
| `payment_value` | decimal |

**Limpieza:** normalización de `payment_type` · validación de dominio (tipos de pago conocidos) ·
verificación de importes no negativos.

---

### 3.5 `reviews`

| | |
|---|---|
| **Origen** | `olist_order_reviews_dataset` |
| **Grano** | Una fila por **relación entre reseña y orden** |
| **Clave primaria** | `(review_id, order_id)` |

**Justificación.** `review_id` **no es único**. Verificado:

```
filas                                    : 99 224
review_id duplicados                     :    814
   · con order_id DISTINTO               :    789
   · con order_id IGUAL (duplicado real) :      0
```

Una misma reseña puede estar asociada a varias órdenes —misma puntuación y misma fecha, órdenes
distintas— probablemente porque el cliente recibió una única encuesta que cubría varios pedidos.
No existe ningún duplicado exacto, por lo que `(review_id, order_id)` identifica cada registro.

> ⚠️ **Advertencia para la capa Gold:** al unir `reviews` con `orders`, una reseña que cubre 2
> órdenes **cuenta dos veces**. Cualquier métrica de puntuación media debe declarar explícitamente
> si el grano de análisis es la reseña o la orden.

| Columna | Tipo |
|---|---|
| `review_id` | string |
| `order_id` | string |
| `review_score` | integer |
| `review_comment_title` | string |
| `review_comment_message` | string |
| `review_creation_date` | timestamp |
| `review_answer_timestamp` | timestamp |

**Limpieza:** conversión de fechas · **el texto libre se conserva íntegro**, incluidos los saltos de
línea embebidos (3 852 comentarios los contienen), para no perder información ni alterar el conteo
de registros.

---

### 3.6 `products`

| | |
|---|---|
| **Origen** | `olist_products_dataset` + `product_category_name_translation` |
| **Grano** | Una fila por **producto** |
| **Clave primaria** | `product_id` (verificada: 32 951 filas, 0 duplicados) |

**Justificación de la integración.** La tabla de traducciones tiene **71 filas** y su único propósito
es traducir la categoría. Es un catálogo de referencia, no una entidad de negocio: se integra
mediante `LEFT JOIN` dentro de `products` conservando **ambos** nombres, el original en portugués y
la traducción al inglés. Un `LEFT JOIN` —no `INNER`— para no perder productos cuya categoría no
figure en el catálogo de traducción.

Las columnas de longitud de nombre y descripción **se conservan**: aunque no las use ningún
requisito actual, son predictores plausibles de conversión y Silver debe mantenerse **neutral
respecto a casos de uso futuros**.

**Limpieza:** normalización de nombres de categoría · tratamiento explícito de productos sin
categoría · conversión de dimensiones y peso a numérico.

---

### 3.7 `sellers`

| | |
|---|---|
| **Origen** | `olist_sellers_dataset` |
| **Grano** | Una fila por **vendedor** |
| **Clave primaria** | `seller_id` (verificada: 3 095 filas, 0 duplicados) |

**Nota sobre BQ-04.** Esta es la entidad que deberá convertirse en **SCD Tipo 2** cuando se
implementen los segmentos de vendedor, ya que BQ-04 exige atribuir las ventas al segmento vigente
**en la fecha de compra**. Requerirá una clave sustituta `seller_sk` y columnas `valid_from`,
`valid_to` e `is_current`.

**Limpieza:** ZIP como texto · estandarización de ciudades · normalización de estados · validación
de claves.

---

### 3.8 `geolocation`

| | |
|---|---|
| **Origen** | `olist_geolocation_dataset` |
| **Grano** | Una fila por **código postal** |
| **Clave primaria** | `geolocation_zip_code_prefix` |
| **Volumen esperado** | ~19 000 filas (desde 1 000 163 en origen) |

**Justificación.** El origen contiene múltiples observaciones por código postal:

```
filas en origen : 1 000 163
duplicados por zip_code_prefix : 981 148
```

Silver consolida cada código postal en **una ubicación representativa** usando la **mediana** de
latitud y longitud. Se eligió la mediana sobre el promedio por ser **robusta frente a valores
atípicos**: una única coordenada mal geocodificada desplazaría el promedio de todo el código postal,
mientras que la mediana la ignora.

| Columna | Tipo |
|---|---|
| `geolocation_zip_code_prefix` | string |
| `geolocation_lat` | double |
| `geolocation_lng` | double |
| `geolocation_city` | string |
| `geolocation_state` | string |

**Limpieza:** consolidación por mediana · ZIP como texto · resolución del valor más frecuente para
ciudad y estado dentro de cada código postal · validación de que las coordenadas caen dentro del
rango geográfico de Brasil.

---

## 4. Relaciones entre entidades

```
                        ┌──────────────┐
                        │  customers   │
                        │  PK: customer_unique_id
                        └──────┬───────┘
                               │ customer_unique_id
                               │
        ┌──────────────┐   ┌───▼──────────┐   ┌──────────────┐
        │   payments   │   │    orders    │   │   reviews    │
        │ PK: order_id │◀──┤ PK: order_id ├──▶│ PK: review_id│
        │   + payment_ │   │              │   │   + order_id │
        │   sequential │   └───┬──────────┘   └──────────────┘
        └──────────────┘       │ order_id
                               │
                        ┌──────▼───────┐
                        │ order_items  │
                        │ PK: order_id + order_item_id
                        └───┬──────┬───┘
                            │      │
              product_id ┌──▼──┐ ┌─▼─────┐ seller_id
                         │prod.│ │sellers│
                         └──┬──┘ └───┬───┘
                            │ zip    │ zip
                         ┌──▼────────▼──┐
                         │ geolocation  │
                         └──────────────┘
```

> **`payments`, `reviews` y `order_items` son hechos de distinto grano que comparten `order_id`.**
> Nunca se unen entre sí directamente: cualquier análisis que los combine debe agregar cada uno a
> un grano común antes de unirlos.

---

## 5. Calidad de datos

Antes de publicar cualquier entidad se ejecutan validaciones automáticas:

| Validación | Acción si falla |
|---|---|
| Unicidad de la clave primaria | 🛑 Detiene el pipeline |
| Ausencia de nulos en la clave primaria | 🛑 Detiene el pipeline |
| Conformidad de tipos de datos | 🛑 Detiene el pipeline |
| Integridad referencial entre entidades | ⚠️ Cuarentena |
| Consistencia de volumen respecto a Bronze | ⚠️ Alerta |

---

## 6. Política de registros inválidos

No todos los errores tienen la misma gravedad, y la respuesta **no depende del tiempo disponible**:
está determinada por el tipo de error.

### Errores estructurales → detención inmediata del pipeline

Indican que el contrato de datos está roto y que **cualquier resultado sería incorrecto**:

- clave primaria duplicada o nula
- columnas obligatorias ausentes
- cambio inesperado de esquema

La capa Gold conserva su última versión válida (*circuit breaker* del **SLA-03**).

### Errores de negocio → cuarentena

El registro individual es inválido, pero el resto del lote es utilizable:

- valores fuera del dominio esperado
- coordenadas geográficas imposibles
- referencias a entidades inexistentes
- incoherencias temporales (entrega anterior a la compra)

Cada registro en cuarentena conserva: **fecha de procesamiento, archivo origen, regla incumplida,
descripción del error y `batch_id`**.

> **Responsable de la revisión:** el equipo de datos revisa la cuarentena **semanalmente**. Un
> crecimiento sostenido escala como incidente al equipo propietario de la fuente. *Una cuarentena
> que nadie revisa es un cementerio: acumula deuda sin generar acción.*

---

## 7. Estrategia de escritura

| Aspecto | Decisión |
|---|---|
| **Formato** | Delta Lake |
| **Particionamiento** | Sin particionar |
| **Modo de escritura** | `overwrite` (reconstrucción completa) |
| **Lectura desde Bronze** | Únicamente `max(ingestion_date)` |

**Formato.** Delta Lake aporta transacciones ACID, versionado, *time travel*, soporte de `MERGE` y
evolución de esquema — capacidades que serán necesarias al implementar SCD Tipo 2 (BQ-04).

**Particionamiento.** Ninguna entidad se particiona. Con el volumen actual, particionar generaría
numerosos archivos pequeños (*small files*) que degradan la lectura sin aportar poda de particiones.
Se revisará cuando el crecimiento lo justifique.

**Escritura.** Cada ejecución reconstruye las entidades por completo. En Delta esta operación es
**atómica**: se publica una versión nueva en el log de transacciones o no se publica nada, de modo
que los consumidores nunca ven estados intermedios.

**Idempotencia.** Garantizada por el propio `overwrite` sobre Delta: reejecutar produce el mismo
resultado sin duplicados. **No es necesario controlar nombres de archivo deterministas** —a
diferencia de Bronze con Parquet plano— porque el log de transacciones determina qué archivos
componen la versión vigente.

**Lectura desde Bronze.** Se lee **solo la partición más reciente**, que representa por definición
el estado actual de la fuente. Leer todas las particiones **reintroduciría registros eliminados en
el origen**, multiplicaría el costo de procesamiento y produciría resultados no deterministas
(¿qué copia gana la deduplicación?).

---

## 8. Problemas conocidos del origen

| # | Problema | Decisión |
|---|---|---|
| 1 | `customer_id` identifica órdenes, no personas | `customers` usa `customer_unique_id`; `orders` arrastra ambos |
| 2 | Una persona puede tener varias ciudades o estados (122 / 39) | Se conserva el valor de la orden más reciente |
| 3 | `review_id` no es único (814 casos) | Clave compuesta `(review_id, order_id)` |
| 4 | 775 órdenes no tienen ítems asociados | `orders` se mantiene como entidad separada |
| 5 | ~981 K coordenadas duplicadas por código postal | Consolidación por mediana |
| 6 | Una orden puede tener varios pagos | `payments` permanece separada (evita *fan trap*) |
| 7 | 3 852 comentarios contienen saltos de línea | Se conservan íntegros; el conteo se hace por registro, no por línea |
| 8 | Las categorías vienen en portugués | Traducción integrada en `products` vía `LEFT JOIN` |

---

## 9. Evolución futura

Incorporables sin modificar el modelo conceptual:

- Cargas incrementales sobre `orders` y `order_items`
- `MERGE` sobre Delta Lake en lugar de reconstrucción completa
- **SCD Tipo 2 en `sellers`** (requisito de BQ-04) y, si se necesita, en `customers`
- Particionamiento físico cuando el volumen lo justifique
- Mantenimiento con `OPTIMIZE` y `VACUUM`
- Publicación de dimensiones y tablas de hechos en Gold
- Pruebas automáticas de calidad con Great Expectations o equivalente
