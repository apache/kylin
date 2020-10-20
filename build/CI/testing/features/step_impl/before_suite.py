from getgauge.python import before_suite
import os
import json

from kylin_utils import util


@before_suite()
def create_generic_model_and_cube():
    client = util.setup_instance('kylin_instance.yml')
    if client.version == '3.x':
        with open(os.path.join('data/generic_desc_data', 'generic_desc_data_3x.json'), 'r') as f:
            data = json.load(f)
    elif client.version == '4.x':
        with open(os.path.join('data/generic_desc_data', 'generic_desc_data_4x.json'), 'r') as f:
            data = json.load(f)

    project_name = client.generic_project
    if not util.if_project_exists(kylin_client=client, project=project_name):
        client.create_project(project_name)

    tables = data.get('load_table_list')
    resp = client.load_table(project_name=project_name, tables=tables)
    assert ",".join(resp["result.loaded"]) == tables

    model_desc_data = data.get('model_desc_data')
    model_name = model_desc_data.get('name')

    if not util.if_model_exists(kylin_client=client, model_name=model_name, project=project_name):
        resp = client.create_model(project_name=project_name, 
                                   model_name=model_name, 
                                   model_desc_data=model_desc_data)
        assert json.loads(resp['modelDescData'])['name'] == model_name

    cube_desc_data = data.get('cube_desc_data')
    cube_name = cube_desc_data.get('name')
    if not util.if_cube_exists(kylin_client=client, cube_name=cube_name, project=project_name):
        resp = client.create_cube(project_name=project_name,
                                  cube_name=cube_name,
                                  cube_desc_data=cube_desc_data)
        assert json.loads(resp['cubeDescData'])['name'] == cube_name
    if client.get_cube_instance(cube_name=cube_name).get('status') != 'READY':
        resp = client.full_build_cube(cube_name=cube_name)
        assert client.await_job_finished(job_id=resp['uuid'], waiting_time=20)
