-- SCD Tipo 2 · Regla 3: los periodos de un mismo vendedor no se traslapan.
--
-- Si dos versiones cubren el mismo día, un join point-in-time devuelve DOS
-- filas por venta e infla el GMV, sin que nada falle.
--
-- `a.seller_sk < b.seller_sk` evita comparar una fila consigo misma y evita
-- reportar el mismo par dos veces (A-B y B-A).
select
    a.seller_id,
    a.seller_sk as version_a,
    b.seller_sk as version_b,
    a.valid_from as desde_a,
    a.valid_to   as hasta_a,
    b.valid_from as desde_b,
    b.valid_to   as hasta_b
from {{ source('gold', 'dim_seller') }} as a
join {{ source('gold', 'dim_seller') }} as b
  on  a.seller_id = b.seller_id
  and a.seller_sk < b.seller_sk
  and a.valid_from <= b.valid_to      -- dos rangos se traslapan si cada uno
  and b.valid_from <= a.valid_to      -- empieza antes de que el otro termine
