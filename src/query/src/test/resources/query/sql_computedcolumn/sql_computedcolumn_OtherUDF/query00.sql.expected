

select count(rlike("LSTG_FORMAT_NAME", '^FP |GTC$')),
       count(rlike("LSTG_FORMAT_NAME",'FP-GT+C*')),
       count(rlike("LSTG_FORMAT_NAME",'FP-(non )?GTC')),
       count(rlike("LSTG_FORMAT_NAME",'FP-[A-Z][^a-z]C'))
from test_kylin_fact