# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.io.base import CSV, FunctionDesc, ParameterDesc


class MeasureCSV(CSV):
    columns = {'value': 0, 'expression': 1, 'return_type': 2}
    not_null_attrs = ['id', 'name', 'function']

    def __init__(self, csv_line):
        CSV.__init__(self, csv_line, ',')
        parameter = ParameterDesc()
        parameter.value = self.get_property(MeasureCSV.columns['value'])
        function = FunctionDesc()
        function.expression = self.get_property(MeasureCSV.columns['expression'])
        function.parameter = parameter
        function.returntype = self.get_property(MeasureCSV.columns['return_type'])
        self.id = None
        self.function = function
        self.name = str(self.function.expression) + '_' + str(self.function.parameter.value)

    def to_measure_desc_json(self):
        json_dict = {'id': self.id, 'name': self.name, 'dependent_measure_ref': None,
                     'function': {'returntype': self.function.returntype, 'expression': self.function.expression,
                                  'parameter': {'type': self.function.parameter.type,
                                                'value': self.function.parameter.value}}}
        return json_dict

    def is_valid(self):
        return self.is_object_valid(MeasureCSV)

    @staticmethod
    def get_from_csv(csv_line, id, type):
        m_csv = MeasureCSV(csv_line)

        m_csv.id = id
        m_csv.function.parameter.type = type

        return m_csv

    @staticmethod
    def get_list_from_csv(csv_line, type):
        if not csv_line: return []

        measure_list = []
        measure_csv_list = csv_line.split(';')
        m_id = 1

        for csv in measure_csv_list:
            m_csv = MeasureCSV.get_from_csv(csv, m_id, type)
            if m_csv:
                measure_list.append(m_csv)
                m_id += 1

        return measure_list
