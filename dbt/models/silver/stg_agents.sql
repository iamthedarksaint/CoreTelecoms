SELECT
    *
FROM {{ source('raw', 'agents') }}