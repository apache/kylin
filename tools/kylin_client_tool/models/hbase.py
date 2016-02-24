# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'


class HBaseMappingDesc:
    """
    python class mapping to org.apache.kylin.cube.model.HBaseMappingDesc
    """

    def __init__(self):
        self.column_family = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        hmd = HBaseMappingDesc()

        # deserialize json for columns
        if json_dict.get('column_family') and type(json_dict.get('column_family')) == list:
            column_family_list = json_dict.get('column_family')
            hmd.column_family = [HBaseColumnFamilyDesc.from_json(column_family) for column_family in column_family_list]

        return hmd

    @staticmethod
    def get_from_measures(measures):
        hbmd = HBaseMappingDesc()
        hbmd.column_family = []

        if not measures: return hbmd

        start, step, measure_cnt = 0, 5, len(measures)
        family_id = 1
        while start < measure_cnt:
            hbcfd = HBaseColumnFamilyDesc()
            hbcfd.name = 'F' + str(family_id)
            hbcfd.columns = []

            hbcd = HBaseColumnDesc()
            hbcd.qualifier = 'M'
            hbcd.measure_refs = []

            for measure in measures[start:start + step]:
                hbcd.measure_refs.append(measure.name)

            # append column instance
            hbcfd.columns.append(hbcd)
            # append column family instance
            hbmd.column_family.append(hbcfd)

            start += step
            family_id += 1

        return hbmd


class HBaseColumnFamilyDesc:
    """
    python class mapping to org.apache.kylin.cube.model.HBaseColumnFamilyDesc
    """

    def __init__(self):
        self.name = None
        self.columns = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        hcfd = HBaseColumnFamilyDesc()

        hcfd.name = json_dict.get('name')
        # deserialize json for columns
        if json_dict.get('columns') and type(json_dict.get('columns')) == list:
            column_list = json_dict.get('columns')
            hcfd.columns = [HBaseColumnDesc.from_json(column) for column in column_list]

        return hcfd


class HBaseColumnDesc:
    """
    python class mapping to org.apache.kylin.cube.model.HBaseColumnDesc
    """

    def __init__(self):
        self.qualifier = None
        self.measure_refs = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        hcd = HBaseColumnDesc()

        hcd.qualifier = json_dict.get('qualifier')
        hcd.measure_refs = json_dict.get('measure_refs')

        return hcd
