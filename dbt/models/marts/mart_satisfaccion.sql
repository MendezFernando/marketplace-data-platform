-- BQ-03 — Impacto del retraso de entrega en la satisfacción del cliente.
--
-- Grano: una fila por (mes de compra, tramo de retraso).
--
-- ⚠️ DOS ADVERTENCIAS DE INTERPRETACIÓN:
--
-- 1. El grano de análisis es LA ORDEN, no la reseña. 814 review_id cubren varias
--    órdenes, así que esas reseñas cuentan una vez por cada orden que abarcan.
--    Es correcto para "¿qué puntuación reciben las órdenes tardías?", pero NO
--    para "¿cuántas reseñas hay?".
--
-- 2. La pregunta original incluye el impacto del CLIMA. Esos datos no están
--    ingeridos, así que este mart responde solo la mitad relativa al retraso.

select
    d.year_month,

    case
        when o.is_late = 0 then 'a tiempo'
        when o.days_late <= 3 then '1-3 dias'
        when o.days_late <= 7 then '4-7 dias'
        else '>7 dias'
    end as tramo_retraso,

    -- Aditivas: permiten recalcular porcentajes correctamente a cualquier nivel
    -- de reagrupación.
    sum(r.review_count) as reviews,
    sum(r.is_negative) as reviews_negativas,
    sum(r.is_positive) as reviews_positivas,

    -- NO aditivas: se promedian, nunca se suman. Si alguien reagrupa este mart,
    -- estos promedios dejan de ser válidos; por eso se guardan también los
    -- contadores de arriba.
    avg(r.review_score) as score_medio,
    avg(r.response_time_hours) as tiempo_respuesta_medio_h,
    avg(o.days_late) as retraso_medio_dias

from {{ source('gold', 'fct_orders') }} as o
inner join {{ source('gold', 'fct_reviews') }} as r
    on o.order_id = r.order_id
inner join {{ source('gold', 'dim_date') }} as d
    on o.purchase_date_sk = d.date_sk

where d.date_sk != -1
    -- IMPRESCINDIBLE: una orden cancelada nunca se entregó, así que `is_late`
    -- vale 0 y caería en el tramo "a tiempo", contaminándolo con órdenes que no
    -- llegaron nunca.
    and o.is_delivered = 1

group by 1, 2
