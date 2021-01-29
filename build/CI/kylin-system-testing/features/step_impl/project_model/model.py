from getgauge.python import step
import os
import json
import pytest
from kylin_utils import util

global client
client = util.setup_instance('kylin_instance.yml')


@step("in project <project_name>, clone model <model_name> and name it <clone_name>")
def check_model_clone(project_name, model_name, clone_name):
    clone = client.clone_model(project_name, model_name, clone_name)
    assert util.if_model_exists(kylin_client=client, model_name=clone_name, project=project_name) == 1
    model_desc = client.list_model_desc(project_name, model_name)
    clone_model_desc = client.list_model_desc(project_name, clone_name)
    check_list = ['fact_table', 'lookups', 'dimensions', 'metrics', 'filter_condition', 'partition_desc']
    for i in range(len(check_list)):
        assert model_desc[0][check_list[i]] == clone_model_desc[0][check_list[i]]


@step("again, in project <project_name>, clone model <model_name> and name it <clone_name>")
def check_clone_duplicated(project_name, model_name, clone_name):
    with pytest.raises(Exception, match=r'Model name .* is duplicated, could not be created.'):
        clone = client.clone_model(project_name, model_name, clone_name)



