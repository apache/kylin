# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.object import JsonSerializableObj
from models.dimension import DimensionDesc
from models.measure import MeasureDesc
from models.rowkey import RowKeyDesc
from models.hbase import HBaseMappingDesc


class CubeDesc(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.cube.model.CubeDesc
    """

    class CubeDescSetting:
        APPEND_COUNT_MEASURE = 'append_count_measure'
        NO_DICTIONARY = 'no_dictionary'
        MANDATORY_DIMENSION = 'mandatory_dimension'
        AGGREGATION_GROUP = 'aggregation_group'
        PARTITION_DATE_COLUMN = 'partition_date_column'
        PARTITION_DATE_START = 'partition_date_start'

        @staticmethod
        def get_value(settings, name, default):
            value = settings.get(name)

            if value is None:
                return default
            if value and type(value) == list and value[0].lower() == 'false':
                return False
            if value and type(value) == list and value[0].lower() == 'true':
                return True

            return value

    def __init__(self):
        JsonSerializableObj.__init__(self)
        self.name = None
        self.description = ''
        self.dimensions = None
        self.measures = None
        self.rowkey = None
        self.notify_list = []
        self.hbase_mapping = None
        self.model_name = ''
        self.retention_range = '0'

        # self.uuid = None
        # self.last_modified = None
        # self.fact_table = None
        # self.null_string = None
        # self.filter_condition = None
        # self.cube_partition_desc = None
        # self.signature = None

    def append_count_measure(self):
        no_count_measure = True
        if self.measures:
            for measure in self.measures:
                if measure.name == '_COUNT_':
                    no_count_measure = False

        if no_count_measure:
            if not self.measures: self.measures = []

            self.measures.append(MeasureDesc.get_count_measure(len(self.measures) + 1))

    def apply_settings(self, settings):
        append_count = CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.APPEND_COUNT_MEASURE, True)
        no_dictionary = CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.NO_DICTIONARY, None)
        mandatory_dimension = CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.MANDATORY_DIMENSION,
                                                                 None)
        aggregation_group = CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.AGGREGATION_GROUP,
                                                               None)

        # apply append_count setting
        if append_count is None or append_count is True:
            self.append_count_measure()
            # update hbase_mapping
            self.hbase_mapping = HBaseMappingDesc.get_from_measures(self.measures)

        # apply no_dictionary setting
        if no_dictionary and type(no_dictionary) == list and self.rowkey.rowkey_columns:
            rowkey_column_cnt = len(self.rowkey.rowkey_columns)
            for no_dictionary_tuple in no_dictionary:
                fields = no_dictionary_tuple.split('/')
                rowkey_size = fields[0]
                column_name = fields[1]
                for i in range(rowkey_column_cnt):
                    if self.rowkey.rowkey_columns[i].column == column_name:
                        self.rowkey.rowkey_columns[i].length = int(rowkey_size)
                        self.rowkey.rowkey_columns[i].dictionary = None

        if aggregation_group and type(aggregation_group) == list:
            agg_groups = []
            for aggregation_group_tuple in aggregation_group:
                fields = aggregation_group_tuple.split('/')
                one_group = []
                for i in range(len(fields)):
                    one_group.append(fields[i])
                agg_groups.append(one_group)
            self.rowkey.aggregation_groups = agg_groups

        # apply mandatory_dimension setting
        if mandatory_dimension and type(
                mandatory_dimension) == list and self.rowkey.rowkey_columns and self.rowkey.aggregation_groups:
            rowkey_column_cnt = len(self.rowkey.rowkey_columns)
            for m_dim in mandatory_dimension:
                for i in range(rowkey_column_cnt):
                    if self.rowkey.rowkey_columns[i].column == m_dim:
                        self.rowkey.rowkey_columns[i].mandatory = True

            agg_group_cnt = len(self.rowkey.aggregation_groups)
            for i in range(agg_group_cnt):
                new_agg_group = []
                for dim in self.rowkey.aggregation_groups[i]:
                    if mandatory_dimension.count(dim) <= 0:
                        new_agg_group.append(dim)
                self.rowkey.aggregation_groups[i] = new_agg_group

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        cd = CubeDesc()

        cd.name = json_dict.get('name')
        cd.description = json_dict.get('description')
        if json_dict.get('dimensions') and type(json_dict.get('dimensions')) == list:
            dimension_list = json_dict.get('dimensions')
            cd.dimensions = [DimensionDesc.from_json(dimension) for dimension in dimension_list]
        if json_dict.get('measures') and type(json_dict.get('measures')) == list:
            measure_list = json_dict.get('measures')
            cd.measures = [MeasureDesc.from_json(measure) for measure in measure_list]
            # deserialize json for rowkey
        cd.rowkey = RowKeyDesc.from_json(json_dict.get('rowkey'))
        cd.notify_list = json_dict.get('notify_list')
        # cd.capacity = json_dict.get('capacity')
        # deserialize json for hbase_mapping
        cd.hbase_mapping = HBaseMappingDesc.from_json(json_dict.get('hbase_mapping'))
        cd.retention_range = json_dict.get('retention_range')
        return cd
        # cd.uuid = json_dict.get('uuid')
        # cd.last_modified = json_dict.get('last_modified')
        # cd.fact_table = json_dict.get('fact_table')
        # cd.null_string = json_dict.get('null_string')
        # cd.filter_condition = json_dict.get('filter_condition')
        # deserialize json for cube_partition_desc
        # cd.cube_partition_desc = CubePartitionDesc.from_json(json_dict.get('cube_partition_desc'))
        # deserialize json for dimensions

        # deserialize json for measures
        # cd.signature = json_dict.get('signature')


class CubeModel(JsonSerializableObj):
    def __init__(self):
        JsonSerializableObj.__init__(self)
        self.name = None
        self.fact_table = None
        self.lookups = []
        self.filter_condition = ''
        self.capacity = 'MEDIUM'
        self.partition_desc = None
        self.last_modified = 0

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None
        cm = CubeModel()

        cm.name = json_dict.get('name')
        cm.fact_table = json_dict.get('json_dict')
        if json_dict.get('lookups') and type(json_dict.get('lookups')) == list:
            lookups_list = json_dict.get('lookups')
            cm.lookups = lookups_list
        cm.filter_condition = json_dict.get('json_dict')
        cm.capacity = json_dict.get('json_dict')
        cm.partition_desc = CubePartitionDesc.from_json(json_dict.get('partition_desc'))
        cm.last_modified = json_dict.get('last_modified')
        return cm


class CubePartitionDesc(JsonSerializableObj):
    """
    python class mapping to org.apache.kylin.cube.model.v1.CubePartitionDesc
    """

    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.partition_date_column = ''
        self.partition_date_start = ''
        self.cube_partition_type = 'APPEND'

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        cpd = CubePartitionDesc()

        cpd.partition_date_column = json_dict.get('partition_date_column')
        cpd.partition_date_start = json_dict.get('partition_date_start')
        cpd.cube_partition_type = json_dict.get('cube_partition_type')

        return cpd

    @staticmethod
    def get_default():
        json_dict = {'partition_date_start': 0, 'cube_partition_type': 'APPEND', 'partition_date_column': None}

        return CubePartitionDesc.from_json(json_dict)

    @staticmethod
    def get_from_setting(settings):
        desc = CubePartitionDesc()

        desc.partition_date_start = \
        CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.PARTITION_DATE_START, [None])[0]
        desc.cube_partition_type = 'APPEND'
        desc.partition_date_column = \
        CubeDesc.CubeDescSetting.get_value(settings, CubeDesc.CubeDescSetting.PARTITION_DATE_COLUMN, [None])[0]
        return desc


class CubeInstance(JsonSerializableObj):
    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.name = None
        self.owner = None
        self.version = None
        self.descName = None
        self.cost = None
        self.status = None
        self.segments = None
        self.create_time = None
        self.size_kb = None
        self.source_records_count = None
        self.source_records_size = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        ci = CubeInstance()

        ci.name = json_dict.get('name')
        ci.owner = json_dict.get('owner')
        ci.version = json_dict.get('version')
        ci.descName = json_dict.get('descName')
        ci.cost = json_dict.get('cost')
        ci.status = json_dict.get('status')
        # deserialize json for segments
        if json_dict.get('segments') and type(json_dict.get('segments')) == list:
            segment_list = json_dict.get('segments')
            ci.segments = [CubeSegment.from_json(segment) for segment in segment_list]
        ci.create_time = json_dict.get('create_time')
        ci.size_kb = json_dict.get('size_kb')
        ci.source_records_count = json_dict.get('source_records_count')
        ci.source_records_size = json_dict.get('source_records_size')

        return ci


class CubeSegment(JsonSerializableObj):
    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.uuid = None
        self.name = None
        self.storage_location_identifier = None
        self.date_range_start = None
        self.date_range_end = None
        self.status = None
        self.size_kb = None
        self.source_records = None
        self.source_records_size = None
        self.last_build_time = None
        self.last_build_job_id = None
        self.create_time = None
        self.binary_signature = None
        self.dictionaries = None
        self.snapshots = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        cs = CubeSegment()

        cs.uuid = json_dict.get('uuid')
        cs.name = json_dict.get('name')
        cs.storage_location_identifier = json_dict.get('storage_location_identifier')
        cs.date_range_start = json_dict.get('date_range_start')
        cs.date_range_end = json_dict.get('date_range_end')
        cs.status = json_dict.get('status')
        cs.size_kb = json_dict.get('size_kb')
        cs.source_records = json_dict.get('source_records')
        cs.source_records_size = json_dict.get('source_records_size')
        cs.last_build_time = json_dict.get('last_build_time')
        cs.last_build_job_id = json_dict.get('last_build_job_id')
        cs.create_time = json_dict.get('create_time')
        cs.binary_signature = json_dict.get('binary_signature')
        cs.dictionaries = json_dict.get('dictionaries')
        cs.snapshots = json_dict.get('snapshots')

        return cs
