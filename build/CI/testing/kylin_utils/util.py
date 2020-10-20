from selenium import webdriver
from yaml import load, loader
import os

from kylin_utils import kylin


def setup_instance(file_name):
    instances_file = os.path.join('kylin_instances/', file_name)
    stream = open(instances_file, 'r')
    for item in load(stream, Loader=loader.SafeLoader):
        host = item['host']
        port = item['port']
        version = item['version']
    return kylin.connect(host=host, port=port, version=version)


def kylin_url(file_name):
    instances_file = os.path.join('kylin_instances/', file_name)
    stream = open(instances_file, 'r')
    for item in load(stream, Loader=loader.SafeLoader):
        host = item['host']
        port = item['port']
    return "http://{host}:{port}/kylin".format(host=host, port=port)


def setup_browser(browser_type):
    if browser_type == "chrome":
        browser = webdriver.Chrome(executable_path="browser_driver/chromedriver")

    if browser_type == "firefox":
        browser = webdriver.Firefox(executable_path="browser_driver/geckodriver")

    if browser_type == "safari":
        browser = webdriver.Safari(executable_path="browser_driver/safaridriver")

    return browser


def if_project_exists(kylin_client, project):
    exists = 0
    resp = kylin_client.list_projects()
    for project_info in resp:
        if project_info.get('name') == project:
            exists = 1
    return exists


def if_cube_exists(kylin_client, cube_name, project=None):
    exists = 0
    resp = kylin_client.list_cubes(project=project)
    if resp is not None:
        for cube_info in resp:
            if cube_info.get('name') == cube_name:
                exists = 1
    return exists


def if_model_exists(kylin_client, model_name, project):
    exists = 0
    resp = kylin_client.list_model_desc(project_name=project, model_name=model_name)
    if len(resp) == 1:
        exists = 1
    return exists
