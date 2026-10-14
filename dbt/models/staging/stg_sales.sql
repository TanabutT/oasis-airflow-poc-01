with source as (
    select * from read_parquet('{{ var("landing_dir") }}/sales/*.parquet')
)

select
    cast(order_id as varchar)                                     as order_id,
    cast(order_date as date)                                      as order_date,
    upper(trim(region))                                           as region,
    cast(quantity as integer)                                     as quantity,
    cast(unit_price as decimal(12, 2))                            as unit_price,
    cast(quantity as integer) * cast(unit_price as decimal(12, 2)) as amount
from source
where order_id is not null
