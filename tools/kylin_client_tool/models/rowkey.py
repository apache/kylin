# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'


class RowKeyDesc:
    """
    python class mapping to org.apache.kylin.cube.model.RowKeyDesc
    """

    def __init__(self):
        self.rowkey_columns = None
        self.aggregation_groups = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        rkd = RowKeyDesc()

        # deserialize json for rowkey_columns
        if json_dict.get('rowkey_columns') and type(json_dict.get('rowkey_columns')) == list:
            rowkey_column_list = json_dict.get('rowkey_columns')
            rkd.rowkey_columns = [RowKeyColDesc.from_json(rowkey_column) for rowkey_column in rowkey_column_list]
        rkd.aggregation_groups = json_dict.get('aggregation_groups')

        return rkd

    @staticmethod
    def get_from_dimensions(dimensions):
        rkd = RowKeyDesc()
        rkd.rowkey_columns = []
        rkd.aggregation_groups = []

        if not dimensions: return rkd

        aggregation_group = []
        for dimension in dimensions:
            rkd.rowkey_columns.append(RowKeyColDesc.get_from_dimension(dimension))
            aggregation_group.append(dimension.column[0])
        rkd.aggregation_groups = [aggregation_group]

        return rkd


class RowKeyColDesc:
    """
    python class mapping to org.apache.kylin.cube.model.RowKeyColDesc
    """

    def __init__(self):
        self.column = None
        self.length = None
        self.dictionary = None
        self.mandatory = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        rkcd = RowKeyColDesc()

        rkcd.column = json_dict.get('column')
        rkcd.length = json_dict.get('length')
        rkcd.dictionary = json_dict.get('dictionary')
        rkcd.mandatory = json_dict.get('mandatory')

        return rkcd

    @staticmethod
    def get_from_dimension(dimension):
        rkcd = RowKeyColDesc()

        if dimension:
            rkcd.column = dimension.column[0]
            rkcd.length = 0
            rkcd.mandatory = False
            rkcd.dictionary = 'true'

        return rkcd
