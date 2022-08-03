-- KE-14621 division between integers
SELECT 5/4 as A,
    5.0/4 AS B,
    5/4.0 AS C,
    5.0/4.0 AS D,
    5 * 1.0 / 4 AS E,
    5 / (4 * 1.0) AS F
from test_account limit 1
