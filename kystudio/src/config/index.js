// 全局配置
import { getFullMapping } from '../util'

let apiUrl
let baseUrl
let regexApiUrl

let pageCount = 10
let bigPageCount = 20
let pageSizes = [10, 20, 50, 100]

let maxExportLength = 100000 // 出厂最低配置下，10万行导出在 10秒以内

let sqlRowsLimit = 100
let sqlStrLenLimit = 2000

let maxFilterAndFilterValues = 100

let tooltipDelayTime = 400
if (process.env.NODE_ENV === 'development') {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
} else {
  apiUrl = '/kylin/api/'
  baseUrl = '/kylin/'
  regexApiUrl = '\\/kylin\\/api\\/'
}
export {
  // http请求
  apiUrl,
  baseUrl,
  regexApiUrl,
  pageCount,
  bigPageCount,
  pageSizes,
  maxExportLength,
  tooltipDelayTime,
  sqlRowsLimit,
  sqlStrLenLimit,
  maxFilterAndFilterValues
}
export const menusData = [
  {
    name: 'query',
    path: '/query',
    icon: 'el-ksd-icon-nav_query_24',
    children: [
      {name: 'insight', path: '/query/insight'},
      {name: 'queryhistory', path: '/query/queryhistory'}
    ]
  },
  {
    name: 'studio',
    path: '/studio',
    icon: 'el-ksd-icon-nav_model_24',
    children: [
      { name: 'source', path: '/studio/source' },
      { name: 'modelList', path: '/studio/model' },
      { name: 'snapshot', path: '/studio/snapshot' }
    ]
  },
  {
    name: 'monitor',
    path: '/monitor',
    icon: 'el-ksd-icon-nav_monitor_24',
    children: [
      {name: 'job', path: '/monitor/job'},
      {name: 'streamingjob', path: '/monitor/streamingJob'}
    ]
  },
  {
    name: 'setting',
    path: '/setting',
    icon: 'el-ksd-icon-nav_setting_24'
  },
  {
    name: 'project',
    path: '/admin/project',
    icon: 'el-ksd-icon-nav_project_24'
  },
  {
    name: 'user',
    path: '/admin/user',
    icon: 'el-ksd-icon-nav_user_management_24'
  },
  {
    name: 'group',
    path: '/admin/group',
    icon: 'el-ksd-icon-nav_user_group_24'
  }
]

let columnTypeIconMap = {
  'boolean': 'el-icon-ksd-type_boolean',
  'varbinary': 'el-icon-ksd-type_varbinary',
  'date': 'el-icon-ksd-type_date',
  'float': 'el-icon-ksd-type_float',
  'decimal': 'el-icon-ksd-type_decimal',
  'double': 'el-icon-ksd-type_double',
  'int': 'el-icon-ksd-type_int',
  'integer': 'el-icon-ksd-type_int',
  'bigint': 'el-icon-ksd-type_bigint',
  'smallint': 'el-icon-ksd-type_int',
  'tinyint': 'el-icon-ksd-type_int',
  'time': 'el-icon-ksd-type_time',
  'timestamp': 'el-icon-ksd-type_timestamp',
  'varchar': 'el-icon-ksd-type_varchar',
  'map': 'el-icon-ksd-type_map',
  'list': 'el-icon-ksd-type_list',
  'interval_day_to_seconds': 'el-icon-ksd-type_interval_day_to_seconds',
  'interval_years_to_months': 'el-icon-ksd-type_interval_years_to_months'
}
export function columnTypeIcon (columnType) {
  columnType = columnType && columnType.replace(/\s*\([^()]+\)\s*/, '').toLowerCase() || ''
  if (columnType) {
    return columnTypeIconMap[columnType]
  }
  return ''
}

export const pageRefTags = {
  indexPager: 'indexPager',
  IndexDetailPager: 'IndexDetailPager',
  tableIndexDetailPager: 'tableIndexDetailPager',
  segmentPager: 'segmentPager',
  streamSegmentPager: 'streamSegmentPager',
  capacityPager: 'capacityPager',
  projectDetail: 'projectDetail',
  sqlListsPager: 'sqlListsPager',
  jobPager: 'jobPager',
  streamingJobPager: 'streamingJobPager',
  queryHistoryPager: 'queryHistoryPager',
  queryResultPager: 'queryResultPager',
  modleConfigPager: 'modleConfigPager',
  batchMeasurePager: 'batchMeasurePager',
  dimensionPager: 'dimensionPager',
  modelListPager: 'modelListPager',
  userPager: 'userPager',
  userGroupPager: 'userGroupPager',
  modelDimensionPager: 'modelDimensionPager',
  modelMeasurePager: 'modelMeasurePager',
  authorityUserPager: 'authorityUserPager',
  authorityTablePager: 'authorityTablePager',
  projectConfigPager: 'projectConfigPager',
  projectPager: 'projectPager',
  confirmSegmentPager: 'confirmSegmentPager',
  addIndexPager: 'addIndexPager',
  indexGroupPager: 'indexGroupPager',
  indexGroupContentShowPager: 'indexGroupContentShowPager',
  tableIndexPager: 'tableIndexPager',
  tableColumnsPager: 'tableColumnsPager',
  statisticsPager: 'statisticsPager',
  subPartitionValuesPager: 'subPartitionValuesPager',
  subPartitionSegmentPager: 'subPartitionSegmentPager',
  snapshotPager: 'snapshotPager',
  basicPorjectConfigPager: 'basicPorjectConfigPager'
}
export const needLengthMeasureType = ['fixed_length', 'fixed_length_hex', 'int', 'integer']
export const permissions = {
  READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
  MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
  OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
  ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
}
export const permissionsMaps = {
  1: 'READ',
  32: 'MANAGEMENT',
  64: 'OPERATION',
  16: 'ADMINISTRATION'
}
export const engineTypeKylin = [
  {name: 'MapReduce', value: 2},
  {name: 'Spark (Beta)', value: 4}
]
export const engineTypeKap = [
  {name: 'MapReduce', value: 100},
  {name: 'Spark (Beta)', value: 98}
]
export const SystemPwdRegex = /^(?=.*\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/
export const NamedRegex = /^\w+$/
export const NamedRegex1 = /^[\u4e00-\u9fa5_\-（()%?）a-zA-Z0-9\s]+$/
export const measureNameRegex = /^[\u4e00-\u9fa5_\-（()%?）.a-zA-Z0-9\s]+$/
export const positiveNumberRegex = /^[1-9][0-9]*$/ // 的正整数
export const DatePartitionRule = [/^date$/, /^timestamp$/, /^string$/, /^bigint$/, /^int$/, /^integer$/, /^varchar/]
export const TimePartitionRule = [/^long$/, /^bigint$/, /^int$/, /^short$/, /^integer$/, /^tinyint$/, /^string$/, /^varchar/, /^char/]
export const SubPartitionRule = [/^tinyint$/, /^smallint$/, /^int$/, /^integer$/, /^bigint$/, /^float$/, /^double$/, /^decimal/, /^timestamp$/, /^date$/, /^varchar/, /^char/, /^boolean$/]
export const StreamingPartitionRule = [/^timestamp$/]
export const IntegerType = ['bigint', 'int', 'integer', 'tinyint', 'smallint', 'int4', 'long8']
// query result 模块使用解析query 结果排序问题
export const IntegerTypeForQueryResult = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL']
export const personalEmail = {
  'qq.com': 'http://mail.qq.com',
  'gmail.com': 'http://mail.google.com',
  'sina.com': 'http://mail.sina.com.cn',
  '163.com': 'http://mail.163.com',
  '126.com': 'http://mail.126.com',
  'yeah.net': 'http://www.yeah.net/',
  'sohu.com': 'http://mail.sohu.com/',
  'tom.com': 'http://mail.tom.com/',
  'sogou.com': 'http://mail.sogou.com/',
  '139.com': 'http://mail.10086.cn/',
  'hotmail.com': 'http://www.hotmail.com',
  'live.com': 'http://login.live.com/',
  'live.cn': 'http://login.live.cn/',
  'live.com.cn': 'http://login.live.com.cn',
  '189.com': 'http://webmail16.189.cn/webmail/',
  'yahoo.com.cn': 'http://mail.cn.yahoo.com/',
  'yahoo.cn': 'http://mail.cn.yahoo.com/',
  'eyou.com': 'http://www.eyou.com/',
  '21cn.com': 'http://mail.21cn.com/',
  '188.com': 'http://www.188.com/',
  'foxmail.com': 'http://www.foxmail.com'
}

export const measuresDataType = [
  'tinyint', 'smallint', 'integer', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'char', 'varchar', 'boolean'
]

export const measureSumAndTopNDataType = ['tinyint', 'smallint', 'integer', 'bigint', 'float', 'double', 'decimal']

export const measurePercenDataType = ['tinyint', 'smallint', 'integer', 'bigint']

export const timeDataType = [
  'timestamp', 'date', 'time', 'datetime'
]

export const dateFormats = [
  {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
  {label: 'yyyyMMdd', value: 'yyyyMMdd'},
  {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
  {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
  {label: 'yyyy/MM/dd', value: 'yyyy/MM/dd'},
  {label: 'yyyy-MM', value: 'yyyy-MM'},
  {label: 'yyyyMM', value: 'yyyyMM'},
  {label: 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', value: 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''}
]

export const dateTimestampFormats = [
  {label: 'TIMESTAMP (SECOND)', value: 'TIMESTAMP SECOND'},
  {label: 'TIMESTAMP (MILLISECOND)', value: 'TIMESTAMP MILLISECOND'}
]

export const timestampFormats = [
  {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
  {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
  {label: 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', value: 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''}
]

// 根据服务端提供在 issue 的列表，做下数组处理，且进行了去重，最后放到这边的 keywordArr 这个变量中
const keywordArr = [
  'abs',
  'acos',
  'add_months',
  'all',
  'allocate',
  'allow',
  'alter',
  'and',
  'any',
  'are',
  'array',
  'array_max_cardinality',
  'as',
  'asc',
  'asensitive',
  'asin',
  'asymmetric',
  'at',
  'atan',
  'atan2',
  'atomic',
  'authorization',
  'avg',
  'begin',
  'begin_frame',
  'begin_partition',
  'between',
  'bigint',
  'binary',
  'bit',
  'blob',
  'boolean',
  'both',
  'by',
  'call',
  'called',
  'cardinality',
  'cascaded',
  'case',
  'cast',
  'ceil',
  'ceiling',
  'century',
  'char',
  'char_length',
  'character',
  'character_length',
  'check',
  'classifier',
  'clob',
  'close',
  'coalesce',
  'collate',
  'collect',
  'column',
  'commit',
  'concat',
  'condition',
  'connect',
  'constraint',
  'contains',
  'convert',
  'corr',
  'corresponding',
  'cos',
  'cot',
  'count',
  'covar_pop',
  'covar_samp',
  'create',
  'cross',
  'cube',
  'cume_dist',
  'current',
  'current_catalog',
  'current_date',
  'current_default_transform_group',
  'current_path',
  'current_role',
  'current_row',
  'current_schema',
  'current_time',
  'current_timestamp',
  'current_transform_group_for_type',
  'current_user',
  'cursor',
  'cycle',
  'date',
  'datediff',
  'day',
  'dayofmonth',
  'dayofweek',
  'dayofyear',
  'deallocate',
  'dec',
  'decimal',
  'declare',
  'default',
  'define',
  'degrees',
  'delete',
  'dense_rank',
  'deref',
  'desc',
  'describe',
  'deterministic',
  'disallow',
  'disconnect',
  'distinct',
  'double',
  'drop',
  'dynamic',
  'each',
  'element',
  'else',
  'empty',
  'end',
  'end_frame',
  'end_partition',
  'end-exec',
  'equals',
  'escape',
  'every',
  'except',
  'exec',
  'execute',
  'exists',
  'exp',
  'explain',
  'extend',
  'external',
  'extract',
  'false',
  'fetch',
  'filter',
  'first_value',
  'float',
  'floor',
  'for',
  'foreign',
  'frame_row',
  'free',
  'from',
  'full',
  'function',
  'fusion',
  'get',
  'global',
  'grant',
  'group',
  'grouping',
  'groups',
  'having',
  'hold',
  'hour',
  'identity',
  'if',
  'ifnull',
  'import',
  'in',
  'indicator',
  'initcap',
  'initial',
  'inner',
  'inout',
  'insensitive',
  'insert',
  'instr',
  'int',
  'integer',
  'intersect',
  'intersection',
  'interval',
  'into',
  'is',
  'isnull',
  'join',
  'lag',
  'language',
  'large',
  'last_value',
  'lateral',
  'lead',
  'leading',
  'left',
  'length',
  'like',
  'like_regex',
  'limit',
  'ln',
  'local',
  'localtime',
  'localtimestamp',
  'log',
  'log10',
  'lower',
  'ltrim',
  'match',
  'match_number',
  'match_recognize',
  'matches',
  'max',
  'measures',
  'member',
  'merge',
  'method',
  'min',
  'minus',
  'minute',
  'mod',
  'modifies',
  'module',
  'month',
  'multiset',
  'national',
  'natural',
  'nchar',
  'nclob',
  'new',
  'next',
  'no',
  'none',
  'normalize',
  'not',
  'now',
  'nth_value',
  'ntile',
  'null',
  'nullif',
  'numeric',
  'occurrences_regex',
  'octet_length',
  'of',
  'offset',
  'old',
  'omit',
  'on',
  'one',
  'only',
  'open',
  'or',
  'order',
  'out',
  'outer',
  'over',
  'overlaps',
  'overlay',
  'parameter',
  'partition',
  'pattern',
  'per',
  'percent',
  'percent_rank',
  'percentile_cont',
  'percentile_disc',
  'period',
  'permute',
  'pi',
  'portion',
  'position',
  'position_regex',
  'power',
  'precedes',
  'precision',
  'prepare',
  'prev',
  'primary',
  'procedure',
  'quarter',
  'radians',
  'rand',
  'rand_integer',
  'range',
  'rank',
  'reads',
  'real',
  'recursive',
  'ref',
  'references',
  'referencing',
  'regr_avgx',
  'regr_avgy',
  'regr_count',
  'regr_intercept',
  'regr_r2',
  'regr_slope',
  'regr_sxx',
  'regr_sxy',
  'regr_syy',
  'release',
  'repeat',
  'replace',
  'reset',
  'result',
  'return',
  'returns',
  'reverse',
  'revoke',
  'right',
  'rlike',
  'rollback',
  'rollup',
  'round',
  'row',
  'row_number',
  'rows',
  'running',
  'savepoint',
  'scope',
  'scroll',
  'search',
  'second',
  'seek',
  'select',
  'sensitive',
  'session_user',
  'set',
  'show',
  'sign',
  'similar',
  'sin',
  'skip',
  'smallint',
  'some',
  'specific',
  'specifictype',
  'split_part',
  'sql',
  'sql_bigint',
  'sql_binary',
  'sql_bit',
  'sql_blob',
  'sql_boolean',
  'sql_char',
  'sql_clob',
  'sql_date',
  'sql_decimal',
  'sql_double',
  'sql_float',
  'sql_integer',
  'sql_nclob',
  'sql_numeric',
  'sql_nvarchar',
  'sql_real',
  'sql_smallint',
  'sql_time',
  'sql_timestamp',
  'sql_tinyint',
  'sql_tsi_day',
  'sql_tsi_frac_second',
  'sql_tsi_hour',
  'sql_tsi_microsecond',
  'sql_tsi_minute',
  'sql_tsi_month',
  'sql_tsi_quarter',
  'sql_tsi_second',
  'sql_tsi_week',
  'sql_tsi_year',
  'sql_varbinary',
  'sql_varchar',
  'sqlexception',
  'sqlstate',
  'sqlwarning',
  'sqrt',
  'start',
  'static',
  'stddev_pop',
  'stddev_samp',
  'stream',
  'submultiset',
  'subset',
  'substr',
  'substring',
  'substring_regex',
  'succeeds',
  'sum',
  'symmetric',
  'system',
  'system_time',
  'system_user',
  'table',
  'tablesample',
  'tan',
  'then',
  'time',
  'timestamp',
  'timestampadd',
  'timestampdiff',
  'timezone_hour',
  'timezone_minute',
  'tinyint',
  'to',
  'to_date',
  'to_timestamp',
  'trailing',
  'translate',
  'translate_regex',
  'translation',
  'treat',
  'trigger',
  'trim',
  'trim_array',
  'true',
  'truncate',
  'ucase',
  'uescape',
  'union',
  'unique',
  'unknown',
  'unnest',
  'update',
  'upper',
  'upsert',
  'user',
  'using',
  'value',
  'value_of',
  'values',
  'var_pop',
  'var_samp',
  'varbinary',
  'varchar',
  'varying',
  'versioning',
  'view',
  'week',
  'when',
  'whenever',
  'where',
  'width_bucket',
  'window',
  'with',
  'within',
  'without',
  'year'
]
// 将 keywordArr 转化为 编辑器所需的格式
export const insightKeyword = keywordArr.map((item) => {
  return {meta: 'keyword', caption: item, value: item, scope: 1}
})

export const assignTypes = [{value: 'user', label: 'user'}, {value: 'group', label: 'group'}]
export const sourceTypes = getFullMapping({
  HIVE: 9,
  RDBMS: 16,
  KAFKA: 1,
  CSV: 13,
  GBASE: 8
})

export const sourceNameMapping = {
  HIVE: 'Hive',
  RDBMS: 'RDBMS',
  KAFKA: 'Kafka',
  RDBMS2: 'RDBMS',
  CSV: 'CSV',
  GBASE: 'GBASE'
}

export const pageSizeMapping = {
  TABLE_TREE: 7
}

// 探测影响模型的c操作类别
export const getAffectedModelsType = {
  TOGGLE_PARTITION: 'TOGGLE_PARTITION',
  DROP_TABLE: 'DROP_TABLE',
  RELOAD_ROOT_FACT: 'RELOAD_ROOT_FACT'
}

export { projectCfgs } from './projectCfgs'
