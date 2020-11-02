
SELECT
    *
FROM {{ source('public', 'raw_state_abbreviations') }}
