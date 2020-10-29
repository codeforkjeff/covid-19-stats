
SELECT
    NAME AS State
    ,Abbreviation AS StateAbbrev
    ,CAST(POPESTIMATE2019 as INT) as Population
FROM raw_nst_population t1
LEFT JOIN raw_state_abbreviations t2
     ON t1.NAME = t2.State
WHERE CAST(t1.STATE AS INT) > 0
