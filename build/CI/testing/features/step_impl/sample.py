from getgauge.python import step, before_spec
from kylin_utils import util
from kylin_utils import equals


@before_spec()
def before_spec_hook():
    global client
    client = util.setup_instance("kylin_instance.yml")


@step("Query sql <select count(*) from kylin_sales> in <project> and compare result with pushdown result")
def query_sql_and_compare_result_with_pushdown_result(sql, project):
    equals.compare_sql_result(sql=sql, project=project, kylin_client=client)
