
SELECT R.natural_key, T0.code, T1.code, T2.code … FROM record R
LEFT OUTER JOIN record_token T0 ON R.natural_key = T0.natural_key AND T0.code = 'type-3'
LEFT OUTER JOIN record_token T1 ON R.natural_key = T1.natural_key AND T1.code = 'type-2'
LEFT OUTER JOIN record_token T2 ON R.natural_key = T2.natural_key AND T2.code = 'type-1'
LIMIT 10;
