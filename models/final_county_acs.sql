
{{
  config({
    "pre-hook": 'drop index if exists idx_{{ this.table }}',
    "post-hook": 'create index if not exists idx_{{ this.table }} on {{ this }} (state_and_county)'
    })
}}

SELECT
    *
    ,state || county AS state_and_county
FROM {{ source('public', 'raw_county_acs') }}

