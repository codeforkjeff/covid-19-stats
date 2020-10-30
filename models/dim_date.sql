
{{
  config({
    "pre-hook": [
      'drop index if exists idx1_{{ this.table }}',
      'drop index if exists idx2_{{ this.table }}',
      'drop index if exists idx3_{{ this.table }}',
      'drop index if exists idx4_{{ this.table }}'
    ],
    "post-hook": [
      'create index if not exists idx1_{{ this.table }} on {{ this }} (Date, Minus1Day)',
      'create index if not exists idx2_{{ this.table }} on {{ this }} (Date, Minus7Days)',
      'create index if not exists idx3_{{ this.table }} on {{ this }} (Date, Minus14Days)',
      'create index if not exists idx4_{{ this.table }} on {{ this }} (Date, Minus30Days)'
    ]
    })
}}

WITH RECURSIVE dates(Date) AS (
  VALUES(to_date('20200101', 'YYYYMMDD'))
  UNION ALL
  SELECT
      (date + interval '1 day')::date
  FROM dates
  WHERE date < date '2020-12-31'
)
SELECT
    date
    ,date - interval '1 day' AS Minus1Day
    ,date - interval '7 days' AS Minus7Days
    ,date - interval '14 days' AS Minus14Days
    ,date - interval '30 days' AS Minus30Days
FROM dates
