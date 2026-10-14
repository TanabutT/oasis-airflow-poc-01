-- Silver: one row per (indicator, topic). Explodes the topics array.
-- Empty placeholder entries like {} are dropped (they have no id).
select distinct
    nullif(trim(b.id), '')                                 as indicator_id,
    try_cast(nullif(trim(t.id), '') as integer)            as topic_id,
    nullif(trim(replace(t.value, '\/', '/')), '')          as topic_name,
    $batch_id                                              as batch_id
from bronze_raw as b, unnest(b.topics) as u(t)
where t.id is not null
