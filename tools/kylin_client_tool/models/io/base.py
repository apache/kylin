# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'


class CSV:
    def __init__(self, csv_line, sep):
        self.csv_line = csv_line
        self.content_list = csv_line.split(sep) if csv_line and isinstance(csv_line, str) else None

    def get_property(self, ind):
        if self.content_list and len(self.content_list) > ind:
            return self.content_list[ind]
        return None

    def is_object_valid(self, cls):
        if hasattr(cls, 'not_null_attrs'):
            for attr in getattr(cls, 'not_null_attrs'):
                if not hasattr(self, attr) or not getattr(self, attr):
                    return False
        return True


class FunctionDesc:
    """
    python class mapping to org.apache.kylin.metadata.model.FunctionDesc
    """

    def __init__(self):
        self.expression = None
        self.parameter = None
        self.returntype = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        fd = FunctionDesc()

        fd.expression = json_dict.get('expression')
        # deserialize json for parameter
        fd.parameter = ParameterDesc.from_json(json_dict.get('parameter'))
        fd.returntype = json_dict.get('returntype')

        return fd


class ParameterDesc:
    """
    python class mapping to org.apache.kylin.metadata.model.ParameterDesc
    """

    def __init__(self):
        self.type = None
        self.value = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        pd = ParameterDesc()

        pd.type = json_dict.get('type')
        pd.value = json_dict.get('value')

        return pd
