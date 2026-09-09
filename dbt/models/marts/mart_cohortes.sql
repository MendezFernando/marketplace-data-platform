-- BQ-02 — Retención y valor de vida del cliente por cohorte.
--
-- Grano: una fila por mes de cohorte (el mes de la PRIMERA compra del cliente).
--
-- Es el único mart que responde su pregunta de negocio al 100%: no depende de
-- ninguna fuente externa sin ingerir.

with actividad as (

    select
        c.cohort_year_month,
        c.customer_sk,
        o.order_value,

        -- Meses transcurridos entre la primera compra del cliente y esta orden.
        -- Se calcula sobre año y mes, no sobre días: "3 meses después" significa
        -- el tercer mes natural, no 90 días exactos.
        (extract(year from d.full_date) - extract(year from c.first_purchase_date)) * 12
        + (extract(month from d.full_date) - extract(month from c.first_purchase_date))
            as meses_desde_cohorte

    from {{ source('gold', 'fct_orders') }} as o
    inner join {{ source('gold', 'dim_customer') }} as c
        on o.customer_sk = c.customer_sk
    inner join {{ source('gold', 'dim_date') }} as d
        on o.purchase_date_sk = d.date_sk

    where d.date_sk != -1
        -- El miembro desconocido no pertenece a ninguna cohorte real.
        and c.customer_sk != '-1'

)

select
    cohort_year_month,

    -- DENOMINADOR: tamaño de la cohorte. Cada cliente aparece al menos una vez
    -- con meses = 0 (su compra inicial), así que el distinct sobre toda la
    -- actividad da exactamente el número de clientes de la cohorte.
    count(distinct customer_sk) as clientes_cohorte,

    -- NUMERADORES: clientes que volvieron a comprar dentro de cada ventana.
    --
    -- `between 1 and N`, no `>= N`: la pregunta es "¿vuelve a comprar EN LOS 3
    -- meses siguientes?". El 1 excluye la compra inicial, que está en el mes 0.
    count(distinct customer_sk) filter (where meses_desde_cohorte between 1 and 3)
        as recompra_3m,
    count(distinct customer_sk) filter (where meses_desde_cohorte between 1 and 6)
        as recompra_6m,
    count(distinct customer_sk) filter (where meses_desde_cohorte between 1 and 12)
        as recompra_12m,

    -- Valor acumulado. El LTV se calcula al consultar como gmv / clientes, nunca
    -- se almacena: es un ratio y dejaría de ser correcto al reagrupar.
    sum(order_value) as gmv_acumulado,
    sum(order_value) filter (where meses_desde_cohorte = 0) as gmv_primera_compra

from actividad
group by 1
order by 1
