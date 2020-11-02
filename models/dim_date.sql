
SELECT
    cast(date AS Date) as Date
    ,cast(Minus1Day AS DATE) AS Minus1Day
    ,cast(Minus7Days AS DATE) AS Minus7Days
    ,cast(Minus14Days AS DATE) AS Minus14Days
    ,cast(Minus30Days AS DATE) AS Minus30Days
FROM {{ source('public', 'raw_date') }}
