-- Silver: one row per indicator. Input view: bronze_raw (raw JSON records).
-- Cleaning: trim whitespace, blank -> NULL, unescape "\/" from the source text, type the ids.
select
    nullif(trim(id), '')                                     as indicator_id,
    nullif(trim(name), '')                                   as indicator_name,
    nullif(trim(unit), '')                                   as unit,
    try_cast(nullif(trim(source.id), '') as integer)         as source_id,
    nullif(trim(source.value), '')                           as source_name,
    nullif(trim(replace(sourceNote, '\/', '/')), '')         as source_note,
    nullif(trim(replace(sourceOrganization, '\/', '/')), '') as source_organization,
    $batch_id                                                as batch_id
from bronze_raw
