# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

import sys, time, datetime
from models.io.cube import CubeCSV
from models.cube import CubeDesc, CubeModel
from models.cube import CubePartitionDesc
from models.hbase import HBaseMappingDesc
from models.rowkey import RowKeyDesc


class CSVReader:
    APPEND_COUNT_MEASURE = 'append_count_measure'

    @staticmethod
    def get_cube_desc_from_csv_line(csv_line, db):
        cube_dic = {}
        cube_csv = CubeCSV(csv_line.strip(), db)

        if not cube_csv.is_valid():
            raise Exception

        cube_desc = CubeDesc()
        model_desc = CubeModel()
        cube_desc.name = cube_csv.name
        cube_desc.model_name = (cube_csv.name).upper()
        cube_desc.dimensions = cube_csv.dimensions
        cube_desc.measures = cube_csv.measures
        if not cube_desc.rowkey:
            cube_desc.rowkey = RowKeyDesc.get_from_dimensions(cube_desc.dimensions)
        if not cube_desc.hbase_mapping:
            # print cube_desc.measures
            cube_desc.hbase_mapping = HBaseMappingDesc.get_from_measures(cube_desc.measures)

        model_desc.name = cube_csv.name
        model_desc.fact_table = (db + '.' + cube_csv.table).upper()
        if not model_desc.partition_desc:
            model_desc.partition_desc = CubePartitionDesc.get_from_setting(settings=cube_csv.settings)
            if not (model_desc.partition_desc.partition_date_column is None):
                model_desc.partition_desc.partition_date_column \
                    = (db + '.' + cube_csv.table + '.' + model_desc.partition_desc.partition_date_column).upper()
                timestamp = int(time.mktime(datetime.datetime.strptime(model_desc.partition_desc.partition_date_start,
                                                                       "%Y-%m-%d").timetuple())) - time.timezone
                model_desc.partition_desc.partition_date_start = timestamp * 1000

        # print cube_desc.rowkey.rowkey_columns[0].dictionary

        # apply settings
        cube_desc.apply_settings(settings=cube_csv.settings)
        if cube_csv.filter is None:
            model_desc.filter_condition = ''
        else:
            model_desc.filter_condition = cube_csv.filter

        cube_dic['cube_desc'] = cube_desc
        cube_dic['model_desc'] = model_desc
        # print cube_desc.rowkey.rowkey_columns[0].dictionary

        return cube_dic

    @staticmethod
    def get_cube_desc_list_from_csv(csv_file, db):
        fd = open(csv_file, 'r')
        csv_lines = fd.readlines()

        cube_desc_list = []

        for csv_line in csv_lines:
            if csv_line:
                try:
                    cube_dic = CSVReader.get_cube_desc_from_csv_line(csv_line, db)

                    cube_desc_list.append(cube_dic)
                except Exception, ex:
                    import traceback

                    traceback.print_exc()
                    print "can't parse this csv for cube", csv_line
                    sys.exit(1)
                    pass

        return cube_desc_list

    @staticmethod
    def get_cube_names_from_csv(csv_file):
        fd = open(csv_file, 'r')
        csv_lines = fd.readlines()

        cube_names_list = []

        for csv_line in csv_lines:
            csv_line = csv_line.strip()
            if csv_line:
                cube_names_list.append(csv_line)

        return cube_names_list
