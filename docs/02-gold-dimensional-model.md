# Diseño del modelo dimensional — capa Gold

> **Estado:** ✅ v1 — aprobado para implementación
> **Autor:** Fernando Méndez
> **Relacionado con:** [`00-business-requirements.md`](00-business-requirements.md) · [`01-silver-design.md`](01-silver-design.md) · [`adr/0001-arquitectura-lakehouse-medallon.md`](adr/0001-arquitectura-lakehouse-medallon.md)

Gold responde *"qué significa"*: es el modelo que consumen los dashboards, la API y
los analistas. A diferencia de Silver —que es neutral y sirve para cualquier uso—
**Gold está modelada con un propósito concreto**: responder las preguntas de negocio
BQ-01 a BQ-05 de forma rápida, inequívoca y consistente entre áreas.

---

## 1. Matriz de bus

La matriz de bus es la herramienta de diseño de Kimball: cruza los **procesos de
negocio** (las filas) con las **dimensiones** (las columnas) y marca cuáles usa cada
proceso.

Su valor está en lo que revela: las dimensiones marcadas en **varios** procesos son
las **dimensiones conformadas**. Deben tener exactamente la misma definición, las
mismas claves y los mismos valores en todos los procesos, porque son el único
terreno donde dos hechos distintos pueden compararse sin producir números falsos.

| Proceso de negocio | Tabla de hechos | `dim_date` | `dim_customer` | `dim_seller` | `dim_product` | `dim_geography` |
|---|---|:---:|:---:|:---:|:---:|:---:|
| Venta de un ítem | `fct_order_items` | ✅ | ✅ | ✅ | ✅ | ✅ |
| Ciclo de vida de la orden | `fct_orders` | ✅ | ✅ | — | — | ✅ |
| Pago de una orden | `fct_payments` | ✅ | ✅ | — | — | — |
| Reseña de una orden | `fct_reviews` | ✅ | ✅ | — | — | — |

**Lectura de la matriz:**


- `dim_date` y `dim_customer` aparecen en los cuatro procesos: son las dimensiones
  conformadas más críticas. Si "cliente" significara algo distinto en ventas y en
  reseñas, sería imposible cruzar ambos análisis.
- `dim_seller` y `dim_product` solo aparecen en `fct_order_items`, porque el vendedor
  y el producto son atributos del **ítem**, no de la orden. Una orden puede contener
  productos de tres vendedores distintos: asociar "un vendedor" a una orden sería
  incorrecto.
- `dim_geography` aparece en ventas y órdenes porque ambas se analizan por región de
  entrega.

---

## 2. Tablas de hechos

### 2.1 `fct_order_items`

| Campo | Valor |
|---|---|
| **Proceso de negocio** | Se vende un producto dentro de una orden |
| **Tipo** | Transaccional (una fila por evento ocurrido) |
| **Grano** | **Una fila por ítem de orden** |
| **Volumen** | ~112 650 filas |
| **Responde a** | BQ-01 (margen por categoría), BQ-04 (GMV por segmento de vendedor) |

**Claves foráneas:**

| Columna | Apunta a | Tipo de clave |
|---|---|---|
| `date_sk` | `dim_date` | Sustituta (entero `YYYYMMDD`) |
| `customer_sk` | `dim_customer` | Sustituta (hash) |
| `seller_sk` | `dim_seller` | Sustituta (hash) — **versión vigente en la fecha de compra** |
| `product_sk` | `dim_product` | Sustituta (hash) |
| `geography_sk` | `dim_geography` | Sustituta (hash) — región del cliente |
| `order_id` | *(ninguna)* | **Dimensión degenerada** |
| `order_item_id` | *(ninguna)* | **Dimensión degenerada** |

> Una **dimensión degenerada** es un identificador del sistema origen que se conserva
> en la tabla de hechos pero **no tiene tabla de dimensión propia**, porque no hay
> ningún atributo descriptivo que colgar de él. `order_id` sirve para agrupar ítems de
> la misma orden y para rastrear un registro hasta el origen, pero no tiene "color",
> "categoría" ni "nombre" que describir. Crear una `dim_order` con una sola columna
> añadiría un join sin aportar nada.

**Medidas:**

| Columna | Aditividad | Cómo se calcula |
|---|---|---|
| `price` | **Aditiva** | `silver.order_items.price` |
| `freight_value` | **Aditiva** | `silver.order_items.freight_value` |
| `gross_revenue` | **Aditiva** | `price + freight_value` |
| `item_count` | **Aditiva** | Constante `1` — permite contar ítems con `SUM` |

Las cuatro son aditivas: pueden sumarse por cualquier dimensión sin producir números
falsos. **No se almacena ningún ratio ni promedio**; se calculan al consultar.

---

### 2.2 `fct_orders`

| Campo | Valor |
|---|---|
| **Proceso de negocio** | Una orden recorre su ciclo de vida: compra → aprobación → transportista → entrega |
| **Tipo** | **Snapshot acumulativo** — una fila por orden, con varias fechas que se van rellenando conforme el proceso avanza |
| **Grano** | **Una fila por orden** |
| **Volumen** | ~99 441 filas |
| **Responde a** | BQ-00 (entregas tardías), BQ-02 (cohortes de retención), BQ-03 (satisfacción) |

Se mantiene **separada de `fct_order_items`** porque los granos son distintos. Además,
775 órdenes no tienen ningún ítem asociado (canceladas o sin disponibilidad) y
desaparecerían si se fusionaran ambas tablas.

**Claves foráneas:**

| Columna | Apunta a |
|---|---|
| `purchase_date_sk` | `dim_date` (fecha de compra) |
| `delivered_date_sk` | `dim_date` (fecha de entrega real; `-1` si no se entregó) |
| `estimated_date_sk` | `dim_date` (fecha prometida) |
| `customer_sk` | `dim_customer` |
| `geography_sk` | `dim_geography` |
| `order_id` | Dimensión degenerada |

> Las tres claves de fecha apuntan a **la misma** `dim_date`. Se llama ***role-playing
> dimension***: una sola tabla de dimensión desempeñando varios papeles según la
> columna que la referencia. No se duplica la tabla; en la herramienta de BI se crean
> tres alias.

**Atributos descriptivos de la propia orden:**

| Columna | Notas |
|---|---|
| `order_status` | `delivered`, `canceled`, `unavailable`, `shipped`, … (8 valores) |

> `order_status` se guarda **directamente en la tabla de hechos** en lugar de crear
> `dim_order_status`. Con 8 valores posibles y ningún atributo adicional que describir,
> una tabla de dimensión solo añadiría un join. Es una decisión pragmática: si mañana
> el negocio quisiera agrupar estados en categorías (`activo` / `terminado` /
> `fallido`), entonces sí merecería su propia dimensión.

**Medidas:**

| Columna | Aditividad | Cómo se calcula |
|---|---|---|
| `days_to_delivery` | **No aditiva** ⚠️ | `delivered_customer_date - purchase_timestamp`, en días |
| `days_late` | **No aditiva** ⚠️ | `delivered_customer_date - estimated_delivery_date`; negativo si llegó antes |
| `is_delivered` | **Aditiva** | `1` si el estado es `delivered`, si no `0` |
| `is_late` | **Aditiva** | `1` si se entregó después de la fecha prometida, si no `0` |
| `order_item_count` | **Aditiva** | Número de ítems de la orden |
| `order_value` | **Aditiva** | Suma de `price + freight_value` de sus ítems |

> ⚠️ **`days_to_delivery` y `days_late` son no aditivas**: sumar días de entrega de
> 100 órdenes no significa nada. Se pueden **promediar**, pero nunca sumar. Se guardan
> igualmente porque el promedio a nivel de grupo sí es una métrica válida y calcularlo
> al vuelo requeriría reprocesar fechas en cada consulta.
>
> **En cambio `is_late` sí es aditiva, y esa es la clave del diseño:** al ser 1 o 0,
> `SUM(is_late)` da el número de órdenes tardías y `COUNT(*)` el total. El porcentaje
> se calcula **al consultar**, ya agregado, y sale correcto siempre. Ese es el patrón
> para toda métrica que sea un porcentaje.

---

### 2.3 `fct_payments`

| Campo | Valor |
|---|---|
| **Proceso de negocio** | Se registra un pago sobre una orden |
| **Tipo** | Transaccional |
| **Grano** | **Una fila por pago** (`order_id` + `payment_sequential`) |
| **Volumen** | ~103 886 filas |
| **Responde a** | BQ-01 (mezcla de métodos de pago y financiación) |

**Permanece separada de `fct_order_items`.** Una orden con 3 ítems pagada en 2 plazos
generaría 6 filas si se unieran, **duplicando los ingresos**. Es el *fan trap* clásico.
Cuando un análisis necesite cruzar ambas, se agrega cada una a grano de orden **antes**
de unirlas (*drill-across*).

| Claves foráneas | |
|---|---|
| `date_sk` | `dim_date` (fecha de compra de la orden) |
| `customer_sk` | `dim_customer` |
| `order_id`, `payment_sequential` | Dimensiones degeneradas |

**Atributo:** `payment_type` (`credit_card`, `boleto`, `voucher`, `debit_card`) —
directamente en el hecho, mismo criterio que `order_status`.

| Medidas | Aditividad |
|---|---|
| `payment_value` | Aditiva |
| `payment_installments` | **No aditiva** — es un número de plazos, se promedia, no se suma |
| `payment_count` | Aditiva (constante `1`) |

---

### 2.4 `fct_reviews`

| Campo | Valor |
|---|---|
| **Proceso de negocio** | Un cliente califica una orden |
| **Tipo** | Transaccional |
| **Grano** | **Una fila por par (reseña, orden)** |
| **Volumen** | ~99 224 filas |
| **Responde a** | BQ-03 (impacto del retraso y del clima en la satisfacción) |

`review_id` no es único: 814 reseñas están asociadas a varias órdenes distintas.

> ⚠️ **Advertencia de uso:** una reseña que cubre dos órdenes aparece en dos filas. Al
> calcular la puntuación media hay que declarar si el grano de análisis es la reseña o
> la orden, porque **los dos números son distintos y ambos son legítimos** según lo que
> se quiera medir.

| Claves foráneas | |
|---|---|
| `creation_date_sk` | `dim_date` |
| `customer_sk` | `dim_customer` |
| `order_id`, `review_id` | Dimensiones degeneradas |

| Medidas | Aditividad |
|---|---|
| `review_score` | **No aditiva** — se promedia, nunca se suma |
| `is_negative` | Aditiva — `1` si la puntuación es 1 o 2 |
| `is_positive` | Aditiva — `1` si la puntuación es 4 o 5 |
| `response_time_hours` | No aditiva |
| `review_count` | Aditiva (constante `1`) |

> Mismo patrón que en `fct_orders`: la puntuación media es no aditiva, así que se
> añaden banderas 0/1 que **sí** lo son. `SUM(is_negative) / COUNT(*)` da el porcentaje
> de reseñas negativas correctamente a cualquier nivel de agregación.

---

## 3. Dimensiones

### 3.1 `dim_date`

| Campo | Valor |
|---|---|
| **Origen** | **Generada por el pipeline**, no viene de ninguna fuente |
| **Grano** | Una fila por día natural |
| **Rango** | 2016-01-01 a 2020-12-31 (~1 827 filas) |
| **Clave sustituta** | `date_sk` — entero con formato `YYYYMMDD` (ej. `20170215`) |
| **Clave natural** | `full_date` (tipo `date`) |
| **Tipo de SCD** | **Tipo 0** — el pasado no cambia nunca |

**Atributos:** `year`, `quarter`, `month`, `month_name`, `week_of_year`, `day_of_month`,
`day_of_week`, `day_name`, `is_weekend`, `year_month` (`2017-02`), `year_quarter`
(`2017-Q1`).

> La clave sustituta es un **entero legible** (`20170215`) en lugar de un hash. Es la
> excepción a la regla: las fechas tienen un orden natural, así que un entero ordenable
> permite filtrar rangos sin unir con la dimensión, y facilita depurar leyendo la tabla
> de hechos a simple vista.
>
> **Se incluye la fila `-1`** para representar "fecha desconocida" — necesaria porque
> las órdenes no entregadas no tienen fecha de entrega.

---

### 3.2 `dim_customer`

| Campo | Valor |
|---|---|
| **Origen en Silver** | `customers` |
| **Grano** | Una fila por persona |
| **Volumen** | ~96 096 filas |
| **Clave sustituta** | `customer_sk` = hash de `customer_unique_id` |
| **Clave natural** | `customer_unique_id` |
| **Tipo de SCD** | **Tipo 1** — se sobrescribe, sin historial |

**Justificación del Tipo 1.** La pregunta decisiva es: *"cuando un cliente se mude de
ciudad, ¿los reportes del año pasado deben seguir mostrándolo en la ciudad antigua?"*
Para BQ-02 (retención por cohortes) la respuesta es **no**: lo que importa es la
identidad de la persona y su fecha de primera compra, no dónde vivía. Aplicar Tipo 2
aquí multiplicaría la complejidad de carga sin responder ninguna pregunta actual.

> Es una decisión **revisable**: si el equipo de logística pidiera "ventas atribuidas a
> la región donde vivía el cliente en ese momento", esta dimensión pasaría a Tipo 2.

**Atributos:** `customer_unique_id`, `customer_city`, `customer_state`,
`customer_zip_code_prefix`, y dos derivados que habilitan BQ-02:

| Atributo derivado | Cómo se calcula | Para qué |
|---|---|---|
| `first_purchase_date` | Fecha mínima de compra de esa persona | Define la **cohorte** |
| `cohort_year_month` | `first_purchase_date` en formato `YYYY-MM` | Agrupa la cohorte directamente |

---

### 3.3 `dim_seller` — SCD Tipo 2

| Campo | Valor |
|---|---|
| **Origen en Silver** | `sellers` + volumen calculado desde `order_items` |
| **Grano** | **Una fila por vendedor y por periodo de vigencia de sus atributos** |
| **Volumen** | ~3 095 filas iniciales, creciente con cada cambio de segmento |
| **Clave sustituta** | `seller_sk` = hash de (`seller_id` + `valid_from`) |
| **Clave natural** | `seller_id` |
| **Tipo de SCD** | **Tipo 2** — historial completo |

**Justificación del Tipo 2.** BQ-04 exige literalmente: *"GMV por segmento atribuido al
segmento vigente en la fecha de compra"*. Si un vendedor era Bronze en enero y hoy es
Gold, las ventas de enero deben contarse como Bronze. **Esta es la única forma de que
un reporte histórico sea reproducible**: sin ella, el mismo informe daría números
distintos cada mes.

**Atributos:**

| Columna | Notas |
|---|---|
| `seller_id` | Clave natural — **se repite** entre versiones |
| `seller_city`, `seller_state` | Ubicación |
| `seller_segment` | `Bronze` / `Silver` / `Gold` según GMV de los últimos 90 días |
| `valid_from` | Fecha desde la que esta versión es válida |
| `valid_to` | Fecha hasta la que fue válida; `9999-12-31` si es la vigente |
| `is_current` | `true` en la versión vigente |

**Ejemplo del comportamiento esperado:**

| `seller_sk` | `seller_id` | `seller_segment` | `valid_from` | `valid_to` | `is_current` |
|---|---|---|---|---|---|
| `a1f3…` | `abc-123` | Bronze | 2017-01-01 | 2017-06-30 | false |
| `b7c2…` | `abc-123` | Silver | 2017-07-01 | 2018-02-14 | false |
| `e9d4…` | `abc-123` | Gold | 2018-02-15 | 9999-12-31 | true |

Un ítem vendido el 2017-03-10 apunta a `seller_sk = a1f3…` y **siempre** dirá "Bronze",
aunque hoy ese vendedor sea Gold.

> `valid_to = 9999-12-31` en lugar de `NULL` es deliberado: permite escribir el filtro
> como `fecha BETWEEN valid_from AND valid_to` sin tener que tratar el `NULL` como caso
> especial en cada consulta.

---

### 3.4 `dim_product`

| Campo | Valor |
|---|---|
| **Origen en Silver** | `products` |
| **Grano** | Una fila por producto |
| **Volumen** | ~32 951 filas |
| **Clave sustituta** | `product_sk` = hash de `product_id` |
| **Clave natural** | `product_id` |
| **Tipo de SCD** | **Tipo 1** |

**Justificación.** Ningún requisito actual necesita saber a qué categoría pertenecía un
producto en el pasado. Si una categoría se corrige, es una **corrección de un error**,
no un cambio de negocio con significado histórico — el caso de uso típico del Tipo 1.

**Atributos:** `product_id`, `product_category_name` (portugués),
`product_category_name_english`, `product_photos_qty`, `product_weight_g`, dimensiones
físicas, y dos derivados:

| Atributo derivado | Cómo se calcula |
|---|---|
| `product_volume_cm3` | `length × height × width` |
| `weight_bucket` | `ligero` (<500 g) / `medio` (500 g–2 kg) / `pesado` (>2 kg) |

---

### 3.5 `dim_geography`

| Campo | Valor |
|---|---|
| **Origen en Silver** | `geolocation` |
| **Grano** | Una fila por código postal |
| **Volumen** | ~19 015 filas |
| **Clave sustituta** | `geography_sk` = hash de `zip_code_prefix` |
| **Clave natural** | `zip_code_prefix` |
| **Tipo de SCD** | **Tipo 1** — la geografía no cambia |

**Atributos:** `zip_code_prefix`, `city`, `state`, `latitude`, `longitude`, y un
derivado: `region` (Norte / Nordeste / Centro-Oeste / Sudeste / Sur), que agrupa los 27
estados brasileños en 5 regiones y permite analizar a un nivel más manejable.

---

## 4. Miembros desconocidos

Cuando un hecho no tiene dimensión asociada —una orden sin fecha de entrega, un
producto sin categoría— hay dos opciones: dejar `NULL` en la clave foránea, o apuntar a
una fila especial.

**Se usa la fila especial con clave `-1`.** El motivo es concreto: con `NULL`, un
`INNER JOIN` **descarta la fila entera** y el hecho desaparece del reporte sin que nadie
lo note. Con el miembro desconocido, la fila sobrevive y el reporte muestra
explícitamente la categoría "Desconocido", que es información y no ausencia.

| Dimensión | Clave del miembro | Valor mostrado | Cuándo aplica |
|---|---|---|---|
| `dim_date` | `-1` | `Fecha desconocida` | Órdenes sin entregar (~3 000) |
| `dim_customer` | `-1` | `Cliente desconocido` | Órdenes sin cliente resoluble |
| `dim_seller` | `-1` | `Vendedor desconocido` | Ítems con vendedor inexistente |
| `dim_product` | `-1` | `Producto desconocido` | Ítems con producto inexistente |
| `dim_geography` | `-1` | `Ubicación desconocida` | Códigos postales sin geolocalización |

---

## 5. Métricas de negocio

Definición única y canónica. Todo ratio se expresa como **numerador ÷ denominador**,
nunca como un valor almacenado.

| Métrica | Definición | Grano de cálculo | Pregunta |
|---|---|---|---|
| **GMV** | `SUM(price)` | Ítem | BQ-01, BQ-04 |
| **Ingresos brutos** | `SUM(price + freight_value)` | Ítem | BQ-01 |
| **Ingreso logístico** | `SUM(freight_value)` | Ítem | BQ-01 |
| **Ticket medio por orden** | `SUM(order_value) / COUNT(DISTINCT order_id)` | Orden | BQ-01 |
| **% de órdenes tardías** | `SUM(is_late) / SUM(is_delivered)` | Orden | BQ-00 |
| **Retraso medio en días** | `AVG(days_late)` **solo sobre `is_late = 1`** | Orden | BQ-00 |
| **Tasa de recompra a N meses** | Clientes de la cohorte con ≥1 orden en N meses ÷ total de la cohorte | Cliente | BQ-02 |
| **Puntuación media** | `AVG(review_score)` | Reseña | BQ-03 |
| **% de reseñas negativas** | `SUM(is_negative) / COUNT(*)` | Reseña | BQ-03 |
| **GMV por segmento** | `SUM(price)` agrupado por `seller_segment` **de la versión vigente en la compra** | Ítem | BQ-04 |

> **Ninguna de estas métricas se almacena calculada.** Se guardan sus componentes
> aditivos y el cociente se resuelve al consultar, después de agregar. Almacenar el
> porcentaje produciría promedios de promedios, que dan números distintos —y erróneos—
> según el nivel de agrupación.

---

## 6. Estrategia de carga

| Aspecto | Decisión | Motivo |
|---|---|---|
| **Orden de carga** | Dimensiones → hechos | Un hecho apunta a claves sustitutas que deben existir antes |
| **Dimensiones Tipo 1** | `overwrite` completo | Reconstruibles desde Silver; simple e idempotente |
| **`dim_seller` (Tipo 2)** | **`MERGE` de Delta Lake** | Debe preservar versiones anteriores; no se puede sobrescribir |
| **Tablas de hechos** | `overwrite` completo | El volumen lo permite y garantiza consistencia total |
| **Claves sustitutas** | **Hash SHA-256** de la clave natural (+ `valid_from` en Tipo 2) | Determinista y calculable en paralelo |
| **Idempotencia** | Garantizada por `overwrite` sobre Delta + claves deterministas | Reejecutar produce el mismo resultado |
| **Destino** | Delta en `s3a://gold/` **y** materialización en PostgreSQL | El lakehouse para análisis; PostgreSQL para Power BI y la API |

**Por qué claves de hash y no enteros secuenciales.** Un entero autoincremental exige
una **secuencia centralizada**: alguien tiene que llevar la cuenta, lo que obliga a
coordinar entre todos los procesos y se convierte en un cuello de botella en un motor
distribuido. Además **no es determinista**: reprocesar generaría claves distintas y
todos los hechos apuntarían al sitio equivocado.

Un hash de la clave natural se calcula **de forma independiente en cada nodo**, sin
coordinación, y da **siempre el mismo resultado** para la misma entrada. El coste es el
tamaño: 32 caracteres frente a 4 bytes de un entero, lo que ocupa más en la tabla de
hechos. A este volumen es irrelevante; a escala de petabytes, sería un trade-off real.

**Materialización en PostgreSQL.** Un lakehouse es eficiente escaneando millones de
filas, pero pobre en consultas pequeñas y concurrentes —que es exactamente el patrón de
Power BI y de la API. Las tablas de Gold son agregados y caben holgadamente en
PostgreSQL, que ofrece SQL estándar, conectores maduros y baja latencia por consulta.

---

## 7. Verificación: ¿el modelo responde a las preguntas?

Cada pregunta de negocio se traza por el modelo antes de implementar nada: qué mido,
por qué agrupo, cómo se unen las tablas, y si la consulta se puede escribir.

| Pregunta | ¿Responde? | Tablas implicadas | Qué falta |
|---|---|---|---|
| **BQ-00** Entregas tardías | ✅ Sí | `fct_orders` + `dim_date` + `dim_geography` | — |
| **BQ-01** Margen por categoría | ⚠️ Parcial | `fct_order_items` + `dim_product` | **Tipo de cambio BRL→USD histórico**. En BRL sí responde; en USD no. |
| **BQ-02** Retención por cohortes | ✅ Sí | `fct_orders` + `dim_customer` | — |
| **BQ-03** Clima y satisfacción | ⚠️ Parcial | `fct_reviews` + `fct_orders` | **Datos meteorológicos por región y fecha**. La parte del retraso sí responde. |
| **BQ-04** Segmento de vendedor | ✅ Sí | `fct_order_items` + `dim_seller` (SCD2) | — |
| **BQ-05** Embudo de compra | ❌ No | *(ninguna)* | **Todo**: clickstream sin ingerir, falta `fct_session_events` y `dim_device` |

**Conclusión:** los tres huecos son **fuentes de datos no ingeridas**, no defectos del
modelo dimensional. Ninguna de las tablas diseñadas necesita rehacerse para cerrarlos;
solo hay que añadir dimensiones y hechos nuevos.

**Ampliaciones necesarias, por orden de esfuerzo:**

| Ampliación | Para | Requiere |
|---|---|---|
| `dim_currency_rate` (una fila por moneda y día) | BQ-01 | Ingerir la API de tipo de cambio a Bronze |
| Atributos de clima en `dim_geography` o hecho propio | BQ-03 | Ingerir la API meteorológica a Bronze |
| `fct_session_events` + `dim_device` | BQ-05 | Generador de clickstream y procesamiento en streaming |

---

## 8. Decisiones abiertas

- **Umbrales del segmento de vendedor.** `Bronze` / `Silver` / `Gold` según GMV de los
  últimos 90 días, pero los cortes concretos son una decisión de negocio pendiente de
  acordar con Seller Management. Valores provisionales: Bronze < 5 000 BRL,
  Silver 5 000–50 000, Gold > 50 000.
- **Frecuencia de recálculo del segmento.** El diseño asume semanal; confirmar.
- **Retención del historial en `dim_seller`.** Sin política de purga, la dimensión
  crece indefinidamente. Definir cuántos años se conservan.
- **Fuentes externas (FX y clima).** BQ-01 requiere conversión a USD y BQ-03 datos
  meteorológicos. Ninguna está aún ingerida; se incorporarán como `dim_currency_rate` y
  atributos de `dim_geography` o un hecho propio.