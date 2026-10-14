select
    order_date,
    region,
    count(distinct order_id) as orders,
    sum(quantity)            as units,
    sum(amount)              as revenue
from {{ ref('stg_sales') }}
group by order_date, region
