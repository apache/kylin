# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'


class DimensionDesc:
    """
    python class mapping to org.apache.kylin.cube.model.DimensionDesc
    """

    def __init__(self):
        self.id = None
        self.name = None
        self.join = None
        self.hierarchy = None
        self.table = None
        self.column = []
        self.datatype = None
        self.derived = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        dd = DimensionDesc()

        dd.id = json_dict.get('id')
        dd.name = json_dict.get('name')
        # deserialize json for join
        dd.join = JoinDesc.from_json(json_dict.get('join'))
        # deserialize json for hierarchy
        if json_dict.get('hierarchy') and type(json_dict.get('hierarchy')) == list:
            hierarchy_list = json_dict.get('hierarchy')
            dd.hierarchy = [HierarchyDesc.from_json(hierarchy) for hierarchy in hierarchy_list]
        dd.table = json_dict.get('table')
        dd.column = json_dict.get('column')
        dd.datatype = json_dict.get('datatype')
        dd.derived = json_dict.get('derived')

        return dd


class JoinDesc:
    """
    python class mapping to org.apache.kylin.metadata.model.JoinDesc
    """

    def __init__(self):
        self.type = None
        self.primary_key = None
        self.foreign_key = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        jd = JoinDesc()

        jd.type = json_dict.get('type')
        jd.primary_key = json_dict.get('primary_key')
        jd.foreign_key = json_dict.get('foreign_key')

        return jd


class HierarchyDesc:
    """
    python class mapping to org.apache.kylin.cube.model.HierarchyDesc
    """

    def __init__(self):
        self.level = None
        self.column = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        hd = HierarchyDesc()

        hd.level = json_dict.get('level')
        hd.column = json_dict.get('column')

        return hd
