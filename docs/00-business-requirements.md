# Requisitos de negocio — Plataforma de datos del marketplace

> **Estado:** ✅ v1 — sujeto a revisión con stakeholders
> **Autor:** Fernando Méndez
> **Fecha:** 2026-07-27

## Contexto

Somos el equipo de datos de un marketplace de e-commerce. Vendedores independientes publican
productos, los clientes compran, y la plataforma gestiona pagos y logística. Hoy el negocio toma
decisiones con exports manuales de la base de datos transaccional. No hay historia, no hay
métricas consistentes y cada área calcula sus números de forma distinta.

Este documento define **qué preguntas debe responder la plataforma** antes de escribir una sola
línea de pipeline. Cada pregunta declara su **grano**, porque el grano de las tablas de hechos se
deriva de aquí y equivocarlo obliga a rehacer todo lo construido encima.

---

## Preguntas de negocio

### BQ-01 — Margen real por categoría, en moneda de reporte

| Campo | Valor |
|---|---|
| **Pregunta** | ¿Cuáles son los ingresos y el margen de contribución por categoría de producto, expresados en USD al tipo de cambio vigente **el día de la compra**, y cómo evolucionan mes a mes? |
| **Stakeholder** | Dirección Financiera (CFO) |
| **Métrica** | `GMV_brl = SUM(price)` · `ingreso_flete_brl = SUM(freight_value)` · `GMV_usd = SUM(price / fx_rate_brl_usd(fecha_compra))` · `margen_contribución = GMV - costo_flete` |
| **Granularidad** | **Una fila por ítem de orden** (`order_id` + `order_item_id`). La categoría es un atributo del producto, no de la orden. |
| **Latencia aceptable** | D-1 (cierre diario). El cierre contable formal es mensual. |
| **Fuentes probables** | `order_items`, `products`, `product_category_translation`, **API externa de tipo de cambio BRL→USD** |

**Por qué importa al negocio:** el marketplace opera en BRL pero reporta a inversionistas en USD.
Hoy Finanzas convierte con el tipo de cambio del día del reporte, lo que hace que los ingresos
históricos "cambien" cada mes y sea imposible comparar períodos. Además, sin margen por categoría
no se puede decidir dónde invertir en captación de vendedores.

---

### BQ-02 — Retención y valor de vida del cliente por cohorte

| Campo | Valor |
|---|---|
| **Pregunta** | De los clientes que hicieron su **primera** compra en un mes dado, ¿qué porcentaje vuelve a comprar a los 3, 6 y 12 meses, y cuánto gastan acumulado en ese período? |
| **Stakeholder** | Head of Growth / Marketing |
| **Métrica** | `tasa_recompra(cohorte, n) = clientes_de_la_cohorte_con_≥1_orden_en_los_n_meses / total_clientes_de_la_cohorte` · `LTV_n = GMV_acumulado / clientes_de_la_cohorte` |
| **Granularidad** | Hecho base: **una fila por orden**. Reporte: **una fila por (mes_cohorte, mes_relativo)**. |
| **Latencia aceptable** | Semanal. Una cohorte madura en meses; actualizarla a diario no cambia ninguna decisión. |
| **Fuentes probables** | `orders`, `customers` (⚠️ usar `customer_unique_id`, **no** `customer_id`), `order_items` |

**Por qué importa al negocio:** el costo de adquisición solo se justifica si el cliente vuelve. Si
la retención a 6 meses es del 3%, el negocio es de adquisición pura y toda la inversión en
programas de fidelidad está mal dirigida. Marketing necesita este número para fijar el CAC máximo.

**⚠️ Trampa conocida de la fuente:** en `customers`, `customer_id` es **único por orden** (es una
clave técnica del sistema transaccional), mientras que `customer_unique_id` identifica a la
persona real. Usar `customer_id` haría que **todo cliente parezca comprar una sola vez** y la
retención dé 0%. Es exactamente el tipo de error que un pipeline sin entender el dominio comete
en silencio.

---

### BQ-03 — Impacto del retraso y del clima en la satisfacción

| Campo | Valor |
|---|---|
| **Pregunta** | ¿Cómo cae el review score promedio en función de los días de retraso respecto a la fecha prometida, y qué parte de los retrasos coincide con eventos climáticos adversos en la región de entrega? |
| **Stakeholder** | Customer Success + Operaciones |
| **Métrica** | `score_promedio` segmentado por tramo de retraso (`a tiempo` / `1-3 días` / `4-7` / `>7`) · `% órdenes tardías con lluvia intensa o alerta meteorológica en destino en la ventana de entrega` |
| **Granularidad** | **Una fila por review** (`review_id`). Ojo: no todas las órdenes tienen review, y una orden puede tener más de uno. |
| **Latencia aceptable** | Semanal. Los reviews llegan con días de rezago tras la entrega (*late-arriving data*). |
| **Fuentes probables** | `order_reviews`, `orders`, `customers`, **API externa de clima histórico por región/fecha** |

**Por qué importa al negocio:** el score afecta el ranking del vendedor y la conversión. Si una
parte relevante de las malas reseñas se explica por clima —fuera del control del vendedor—, la
política de penalización actual es injusta y expulsa vendedores buenos. Esto habilita ajustar la
fecha prometida al checkout según estacionalidad y región.

**⚠️ Nota metodológica:** esto mide **correlación**, no causalidad. El documento no debe prometer
que el clima *causa* malas reseñas.

---

### BQ-04 — Desempeño por segmento de vendedor a lo largo del tiempo

| Campo | Valor |
|---|---|
| **Pregunta** | ¿Cómo se distribuye el GMV entre segmentos de vendedor (Bronze / Silver / Gold, según volumen de los últimos 90 días), y qué proporción de vendedores asciende o desciende de segmento cada trimestre? |
| **Stakeholder** | Head of Seller Management |
| **Métrica** | `GMV por segmento` **atribuido al segmento vigente en la fecha de compra** · `tasa_promoción` y `tasa_degradación` trimestral |
| **Granularidad** | **Una fila por ítem de orden**, enriquecida con el segmento que el vendedor tenía **ese día**. |
| **Latencia aceptable** | D-1 para el GMV; recálculo de segmento semanal. |
| **Fuentes probables** | `order_items`, `sellers`, `orders` |

**Por qué importa al negocio:** el equipo de vendedores gestiona a los Gold con ejecutivo dedicado
y a los Bronze con autoservicio. Necesitan saber si el programa hace ascender vendedores o si el
segmento es una etiqueta estática que nadie cruza.

**⚠️ Requisito técnico crítico (esta pregunta define la arquitectura):** si un vendedor era Bronze
en enero y hoy es Gold, las ventas de enero deben contarse como **Bronze**. La base transaccional
solo guarda el estado *actual* — sobrescribe. Esto obliga a la plataforma a **conservar la
historia de la dimensión de vendedores (Slowly Changing Dimension Tipo 2)**. Sin esto, cada
recálculo del pasado da un número distinto y los reportes históricos dejan de ser reproducibles.

---

### BQ-05 — Fugas del embudo de compra y recuperación de carrito

| Campo | Valor |
|---|---|
| **Pregunta** | ¿Cuál es la tasa de conversión en cada paso del embudo (vista de producto → añadir al carrito → checkout → pago confirmado) y en qué paso se produce la mayor caída, por dispositivo y categoría? |
| **Stakeholder** | Product Manager de Checkout |
| **Métrica** | `conversión_paso_n = sesiones_que_alcanzan_paso_n / sesiones_que_alcanzan_paso_n-1` · `tasa_abandono_carrito` |
| **Granularidad** | **Una fila por evento de sesión** (`session_id`, `event_type`, `event_timestamp`). Es el grano más fino de toda la plataforma. |
| **Latencia aceptable** | **Near-real-time (< 5 minutos).** Justificación: habilita disparar un email de recuperación mientras la intención de compra sigue viva. La acción es automática e inmediata, por eso la baja latencia sí se paga sola. |
| **Fuentes probables** | Stream de eventos de clickstream (generador sintético) |

**Por qué importa al negocio:** el GMV se puede mover sin traer un solo cliente nuevo si se tapan
las fugas del embudo. Un punto de conversión en checkout vale más que meses de campañas.

**⚠️ Nota de volumen:** esta es la única fuente con volumen real (millones de eventos). Es la que
justifica procesamiento distribuido; las demás caben en memoria.

---

## SLAs de la plataforma

| ID | Compromiso | Métrica de cumplimiento | Consecuencia si se incumple |
|---|---|---|---|
| **SLA-01** — Frescura | Los modelos Gold del día D-1 están disponibles y validados antes de las **07:00** hora local, todos los días | `max(fecha_carga)` ≥ D-1 medido a las 07:00, cumplido el **99%** de los días de cada trimestre | Alerta automática a `#data-alerts`; si a las 09:00 sigue sin resolverse, se notifica a los stakeholders que los dashboards muestran datos desactualizados |
| **SLA-02** — Completitud | El **100%** de las órdenes en estado `delivered` presentes en Bronze para el día D-1 están reflejadas en la capa Gold | Reconciliación diaria de conteos Bronze vs. Gold; diferencia tolerada = 0 filas | El pipeline **falla y no publica** (los datos incompletos no llegan a negocio); ticket automático al equipo de datos |
| **SLA-03** — Calidad | Cero duplicados en las claves primarias de Silver y menos de **0.5%** de nulos en columnas críticas (`order_id`, `customer_unique_id`, `price`, fechas de entrega) | Suite de tests de calidad ejecutada en cada corrida, con umbrales versionados en el repositorio | *Circuit breaker*: la corrida se detiene antes de propagar datos corruptos a Gold. La capa Gold conserva la última versión válida |

**Nota de diseño:** los SLAs no son aspiraciones, son compromisos **medibles automáticamente**.
Ninguno promete 100% de disponibilidad: los pipelines fallan y las APIs externas caen. El margen
(el 1% de SLA-01) es el *error budget* y es lo que hace el compromiso sostenible.

---

## Fuera de alcance (v1)

Tan importante como lo que sí vamos a hacer. Un alcance sin límites es un proyecto que no termina.

- **NLP sobre el texto de los reviews** (análisis de sentimiento, extracción de temas). Usamos el
  score numérico. El texto se conserva en Bronze para un análisis futuro.
- **Modelos predictivos** (predicción de demanda, de churn, de retraso). La plataforma debe
  *habilitarlos* entregando features limpias, pero entrenar modelos no es alcance de v1.
- **Datos personales identificables (PII) reales.** El dataset viene anonimizado y así se mantiene;
  no se implementa un marco de cumplimiento GDPR/LGPD completo, solo la clasificación de columnas
  sensibles en el catálogo.
- **Integración con el ERP financiero.** El cierre contable oficial sigue en el ERP; la plataforma
  entrega analítica, no contabilidad de registro.
- **Recomendador de productos.** Fuera de alcance total.
