# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'
from models.io.base import FunctionDesc


class MeasureDesc:
    """
    python class mapping to org.apache.kylin.metadata.model.MeasureDesc
    """

    def __init__(self):
        self.id = None
        self.name = None
        self.function = None
        self.dependent_measure_ref = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        md = MeasureDesc()

        md.id = json_dict.get('id')
        md.name = json_dict.get('name')
        # deserialize json for function
        md.function = FunctionDesc.from_json(json_dict.get('function'))
        md.dependent_measure_ref = json_dict.get('dependent_measure_ref')

        return md

    @staticmethod
    def get_count_measure(id):
        json_dict = {'id': id, 'name': '_COUNT_',
                     'function': {'expression': 'COUNT', 'returntype': 'bigint',
                                  'parameter': {'type': 'constant', 'value': 1}},
                     'dependent_measure_ref': None}

        return MeasureDesc.from_json(json_dict)
