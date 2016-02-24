# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.io.base import CSV
from models.io.dimension import DimensionCSV
from models.io.measure import MeasureCSV


class CubeCSV(CSV):
    columns = {'name': 0, 'table': 1, 'dimensions': 2, 'measures': 3, 'settings': 4, 'filter': 5}
    not_null_attrs = ['name', 'table', 'dimensions']

    def __init__(self, csv_line, db):
        CSV.__init__(self, csv_line, '|')

        self.name = self.get_property(CubeCSV.columns['name'])
        self.table = self.get_property(CubeCSV.columns['table'])
        self.dimensions = None
        self.measures = None
        self.settings = {}
        self.filter = None

        dimensions_csv = self.get_property(CubeCSV.columns['dimensions'])
        self.dimensions = DimensionCSV.get_list_from_csv(dimensions_csv, self.table, db)

        measures_csv = self.get_property(CubeCSV.columns['measures'])
        self.measures = MeasureCSV.get_list_from_csv(measures_csv, 'column')
        # print measures_csv, len(self.measures) if self.measures else 0

        settings_csv = self.get_property(CubeCSV.columns['settings'])
        self.settings = CubeSettingCSV.get_settings_from_csv(settings_csv)

        filter_csv = self.get_property(CubeCSV.columns['filter'])
        self.filter = filter_csv

    def is_valid(self):
        if not self.is_object_valid(CubeCSV):
            print self.to_cube_desc_json(), 'not ok'
            return False

        if self.dimensions:
            dim_name_dict = {}
            for dimension in self.dimensions:
                if not dimension.is_valid() or dimension.name in dim_name_dict:
                    print dimension.to_dimension_desc_json(), 'not ok'
                    return False
                dim_name_dict[dimension.name] = True

        if self.measures:
            measure_name_dict = {}
            for measure in self.measures:
                if not measure.is_valid() or measure.name in measure_name_dict:
                    print measure.to_measure_desc_json(), 'not ok'
                    return False
                measure_name_dict[measure.name] = True

        return True

    def to_cube_desc_json(self):
        json_dict = {'name': self.name, 'fact_table': self.table, 'capacity': 'MEDIUM',
                     'dimensions': [dimension.to_dimension_desc_json() for dimension in self.dimensions],
                     'measures': [measure.to_measure_desc_json() for measure in self.measures]}

        return json_dict


class CubeSettingCSV(CSV):
    columns = {'name': 0, 'value': 1}

    def __init__(self, csv_line):
        CSV.__init__(self, csv_line, '=')

        self.name = self.get_property(CubeSettingCSV.columns['name'])
        self.value = self.get_property(CubeSettingCSV.columns['value'])

        if self.value:
            self.value = self.value.split(',')

    @staticmethod
    def get_settings_from_csv(csv_line):
        if not csv_line: return {}

        settings = {}
        setting_csv_list = csv_line.split(';')
        for csv in setting_csv_list:
            setting_csv = CubeSettingCSV(csv)
            if setting_csv:
                settings[setting_csv.name] = setting_csv.value

        return settings
