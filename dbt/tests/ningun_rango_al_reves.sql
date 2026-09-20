-- SCD Tipo 2 · Regla 2: ningún periodo de vigencia termina antes de empezar.
--
-- Un rango invertido es la firma de haber procesado una fecha ANTERIOR a la
-- última aplicada: el MERGE cierra la versión vigente con `as_of - 1 día`,
-- que queda antes de su propio `valid_from`.
select
    seller_sk,
    seller_id,
    valid_from,
    valid_to
from {{ source('gold', 'dim_seller') }}
where valid_to < valid_from
