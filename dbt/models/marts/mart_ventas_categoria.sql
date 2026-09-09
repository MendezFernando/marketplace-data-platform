-- BQ-01 — Ingresos y margen de contribución por categoría de producto.
--
-- Grano: una fila por (mes de compra, categoría de producto).
--
-- ⚠️ LIMITACIÓN CONOCIDA: la pregunta de negocio pide los importes en USD al tipo
-- de cambio del día de la compra. La API de tipo de cambio no está ingerida, así
-- que TODOS LOS IMPORTES DE ESTE MART ESTÁN EN BRL. Ver §7 del diseño dimensional.

select
    d.year_month,
    d.year,
    -- Un NULL en una columna de agrupación se comporta de forma inconsistente en
    -- las herramientas de BI: se convierte en una etiqueta explícita.
    coalesce(p.product_category_name_english, 'Sin categoria') as product_category,

    -- Todas las medidas son aditivas: se pueden sumar por cualquier dimensión.
    sum(o.price) as gmv,
    sum(o.freight_value) as ingreso_flete,
    sum(o.gross_revenue) as ingreso_bruto,
    sum(o.price) - sum(o.freight_value) as margen_contribucion,

    -- sum(item_count) en vez de count(order_item_id): `item_count` es una medida
    -- diseñada para contarse y siempre vale 1, mientras que count() saltaría en
    -- silencio cualquier fila con la columna nula.
    sum(o.item_count) as items_vendidos,
    count(distinct o.order_id) as ordenes

from {{ source('gold', 'fct_order_items') }} as o
inner join {{ source('gold', 'dim_product') }} as p
    on o.product_sk = p.product_sk
inner join {{ source('gold', 'dim_date') }} as d
    on o.date_sk = d.date_sk

-- El miembro desconocido (-1) no pertenece a ningún mes y distorsionaría la serie.
where d.date_sk != -1

group by 1, 2, 3
