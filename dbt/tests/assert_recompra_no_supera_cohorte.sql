-- Test SINGULAR (a diferencia de los genéricos declarados en YAML).
--
-- Es SQL puro que devuelve las filas que INCUMPLEN la regla: si devuelve 0 filas
-- el test pasa, si devuelve alguna falla. Se usa cuando la regla es específica de
-- un modelo y no encaja en unique / not_null / accepted_values / relationships.
--
-- La regla aquí es de negocio, no técnica: los clientes que recompran nunca
-- pueden ser más que los clientes de la cohorte. Si esto falla, el denominador
-- está mal calculado y todas las tasas de retención serían falsas.

select
    cohort_year_month,
    clientes_cohorte,
    recompra_3m,
    recompra_6m,
    recompra_12m

from {{ ref('mart_cohortes') }}

where recompra_3m > clientes_cohorte
    or recompra_6m > clientes_cohorte
    or recompra_12m > clientes_cohorte
    -- Las ventanas son acumulativas: lo que cabe en 3 meses cabe en 6 y en 12.
    or recompra_3m > recompra_6m
    or recompra_6m > recompra_12m
