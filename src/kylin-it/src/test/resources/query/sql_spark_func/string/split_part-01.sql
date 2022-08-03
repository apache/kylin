select split_part(LSTG_FORMAT_NAME,'I',1), split_part('a-b-c', '-', 1), split_part('a-b-c', '-', -1), split_part('a-b-c', '-', 100)
from test_kylin_fact