# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.object import JsonSerializableObj
from models.cube import CubeDesc, CubeModel


class CubeRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.CubeRequest
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.uuid = None
        self.cubeName = None
        self.cubeDescData = None
        self.modelDescData = None
        self.successful = None
        self.message = None
        self.cubeDescName = None
        self.project = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        cr = CubeRequest()

        cr.uuid = json_dict.get('uuid')
        cr.cubeName = json_dict.get('cubeName')
        cr.cubeDescData = json_dict.get('cubeDescData')
        cr.modelDescData = json_dict.get('modelDescData')
        cr.successful = json_dict.get('successful')
        cr.message = json_dict.get('message')
        cr.cubeDescName = json_dict.get('cubeDescName')
        cr.project = json_dict.get('project')

        return cr

    @staticmethod
    def get_cube_request_from_cube_desc(cube_desc, model_desc, project=None):
        if not cube_desc or not isinstance(cube_desc, CubeDesc): return None
        if not model_desc or not isinstance(model_desc, CubeModel): return None

        cr = CubeRequest()

        # cr.uuid = cube_desc.uuid
        cr.cubeDescData = cube_desc.to_json()
        # print cr.cubeDescData
        cr.modelDescData = model_desc.to_json()
        cr.cubeName = cube_desc.name

        cr.project = project

        return cr


class JobBuildRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.JobBuildRequest
    """
    BUILD = 'BUILD'
    MERGE = 'MERGE'

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.startTime = None
        self.endTime = None
        self.buildType = JobBuildRequest.BUILD


class JobListRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.JobListRequest
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.cubeName = None
        self.projectName = None
        self.offset = None
        self.limit = None
        self.status = None

    def to_query_string(self):
        qs = ""

        if self.cubeName:
            qs += "cubeName=" + self.cubeName + "&"
        if self.projectName:
            qs += "projectName=" + self.projectName + "&"
        if self.offset is not None:
            qs += "offset=" + str(self.offset) + "&"
        if self.limit is not None:
            qs += "limit=" + str(self.limit) + "&"
        if self.status:
            for status in self.status:
                qs += "status=" + str(status) + "&"

        return qs[:-1] if qs else ""


class ProjectRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.CreateProjectRequest
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.name = None
        self.description = None


class SQLRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.SQLRequest
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.sql = None
        self.project = None
        self.offset = None
        self.limit = None
        self.acceptPartial = None


class PrepareSqlRequest(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.PrepareSqlRequest
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.sql = None
        self.project = None
        self.offset = None
        self.limit = None
        self.acceptPartial = None
        self.params = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        psr = PrepareSqlRequest()

        psr.sql = json_dict.get('sql')
        psr.project = json_dict.get('project')
        psr.offset = json_dict.get('offset')
        psr.limit = json_dict.get('limit')
        psr.acceptPartial = json_dict.get('acceptPartial')
        if json_dict.get('params') and type(json_dict.get('params')) == list:
            param_list = json_dict.get('params')
            psr.params = [StateParam.from_json(param) for param in param_list]

        return psr


class StateParam(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.rest.request.PrepareSqlRequest.StateParam
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.className = None
        self.value = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        sp = StateParam()

        sp.className = json_dict.get('className')
        sp.value = json_dict.get('value')

        return sp
