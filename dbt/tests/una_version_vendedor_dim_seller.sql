-- SCD Tipo 2 · Regla 1: cada vendedor tiene EXACTAMENTE una versión vigente.
--
-- Cero versiones vigentes = el vendedor "desapareció" del presente.
-- Dos o más = un MERGE abrió una versión sin cerrar la anterior; cualquier join
-- con los hechos duplicaría ventas.
select
    seller_id,
    count(*) as versiones_vigentes
from {{ source('gold', 'dim_seller') }}
where is_current
  and seller_sk <> '-1'          -- el miembro desconocido no representa a un vendedor real
group by seller_id
having count(*) <> 1
