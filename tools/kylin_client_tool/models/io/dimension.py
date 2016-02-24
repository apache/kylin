# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.io.base import CSV


class DimensionCSV(CSV):
    columns = {'column': 0, 'data_type': 1}
    not_null_attrs = ['id', 'table', 'column', 'name']

    def __init__(self, csv_line, db):
        CSV.__init__(self, csv_line, ',')

        self.id = None
        self.table = None
        self.column = []
        self.column.append(self.get_property(DimensionCSV.columns['column']))
        self.data_type = self.get_property(DimensionCSV.columns['data_type'])
        self.name = self.column[0]

    def to_dimension_desc_json(self):
        json_dict = {'id': self.id, 'table': self.table, 'datatype': self.data_type,
                     'column': self.column, 'name': self.name}

        return json_dict

    def is_valid(self):
        return self.is_object_valid(DimensionCSV)

    @staticmethod
    def get_from_csv(csv_line, id, table, db):
        d_csv = None

        if csv_line:
            d_csv = DimensionCSV(csv_line, db)
            d_csv.id = id
            d_csv.table = db + '.' + table
            d_csv.name = d_csv.table + '.' + d_csv.name

        return d_csv

    @staticmethod
    def get_list_from_csv(csv_line, table, db):
        if not csv_line: return []

        dimension_list = []
        dimension_csv_list = csv_line.split(';')
        dim_id = 1
        for csv in dimension_csv_list:
            dim_csv = DimensionCSV.get_from_csv(csv, dim_id, table, db)
            if dim_csv:
                dimension_list.append(dim_csv)
                dim_id += 1

        return dimension_list
