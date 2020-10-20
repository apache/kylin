import logging
from kylin_utils import util

_array_types = (list, tuple, set)
_object_types = (dict, )


def api_response_equals(actual, expected, ignore=None):
    if ignore is None:
        ignore = []

    def _get_value(ignore):
        def get_value(key, container):
            if isinstance(container, _object_types):
                return container.get(key)
            if isinstance(container, _array_types):
                errmsg = ''
                for item in container:
                    try:
                        api_response_equals(item, key, ignore=ignore)
                        return item
                    except AssertionError as e:
                        errmsg += str(e) + '\n'
                raise AssertionError(errmsg)

            return None

        return get_value

    getvalue = _get_value(ignore)
    assert_failed = AssertionError(
        f'assert json failed, expected: [{expected}], actual: [{actual}]')

    if isinstance(expected, _array_types):
        if not isinstance(actual, _array_types):
            raise assert_failed
        for item in expected:
            api_response_equals(getvalue(item, actual), item, ignore=ignore)

    elif isinstance(expected, _object_types):
        if not isinstance(actual, _object_types):
            raise assert_failed
        for key, value in expected.items():
            if key not in ignore:
                api_response_equals(getvalue(key, actual),
                                    value,
                                    ignore=ignore)
            else:
                if key not in actual:
                    raise assert_failed
    else:
        if actual != expected:
            raise assert_failed


INTEGER_FAMILY = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT']

FRACTION_FAMILY = ['DECIMAL', 'DOUBLE', 'FLOAT']

STRING_FAMILY = ['CHAR', 'VARCHAR', 'STRING']


def _is_family(datatype1, datatype2):
    if datatype1 in STRING_FAMILY and datatype2 in STRING_FAMILY:
        return True
    if datatype1 in FRACTION_FAMILY and datatype2 in FRACTION_FAMILY:
        return True
    if datatype1 in INTEGER_FAMILY and datatype2 in INTEGER_FAMILY:
        return True
    return datatype1 == datatype2


class _Row(tuple):
    def __init__(self, values, types):  # pylint: disable=unused-argument
        tuple.__init__(self)
        if len(values) != len(types):
            raise ValueError('???')

        self._types = types

        self._has_fraction = False
        for datatype in self._types:
            if datatype in FRACTION_FAMILY:
                self._has_fraction = True

    def __new__(cls, values, types):  # pylint: disable=unused-argument
        return tuple.__new__(cls, values)

    def __eq__(self, other):
        if not self._has_fraction or not other._has_fraction:
            return tuple.__eq__(self, other)

        if len(self._types) != len(other._types):
            return False

        for i in range(len(self._types)):
            stype = self._types[i]
            otype = other._types[i]

            if not _is_family(stype, otype):
                return False

            svalue = self[i]
            ovalue = other[i]

            if stype in FRACTION_FAMILY:
                fsvalue = float(svalue)
                fovalue = float(ovalue)

                diff = abs(fsvalue - fovalue)

                rate = diff / min(fsvalue, fovalue
                                  ) if fsvalue != 0 and fovalue != 0 else diff
                if abs(rate) > 0.01:
                    return False

            else:
                if svalue != ovalue:
                    return False

        return True

    def __hash__(self):
        # Always use __eq__ to compare
        return 0


def query_result_equals(expect_resp, actual_resp):
    expect_column_types = [
        x['columnTypeName'] for x in expect_resp['columnMetas']
    ]
    expect_result = [[y.strip() if y else y for y in x]
                     for x in expect_resp['results']]

    actual_column_types = [
        x['columnTypeName'] for x in actual_resp['columnMetas']
    ]
    actual_result = [[y.strip() if y else y for y in x]
                     for x in actual_resp['results']]

    if len(expect_column_types) != len(actual_column_types):
        logging.error('column count assert failed [%s,%s]',
                      len(expect_column_types), len(actual_column_types))
        return False

    return dataset_equals(expect_result, actual_result, expect_column_types,
                          actual_column_types)


def dataset_equals(expect,
                   actual,
                   expect_col_types=None,
                   actual_col_types=None):
    if len(expect) != len(actual):
        logging.error('row count assert failed [%s,%s]', len(expect),
                      len(actual))
        return False

    if expect_col_types is None:
        expect_col_types = ['VARCHAR'] * len(expect[0])
    expect_set = set()
    for values in expect:
        expect_set.add(_Row(values, expect_col_types))

    if actual_col_types is None:
        actual_col_types = expect_col_types if expect_col_types else [
            'VARCHAR'
        ] * len(actual[0])
    actual_set = set()
    for values in actual:
        actual_set.add(_Row(values, actual_col_types))

    assert_result = expect_set ^ actual_set
    if assert_result:
        logging.error('diff[%s]', len(assert_result))
        if len(assert_result) < 20:
            print(assert_result)
        return False

    return True


def compare_sql_result(sql, project, kylin_client, cube=None):
    pushdown_project = kylin_client.pushdown_project
    if not util.if_project_exists(kylin_client=kylin_client, project=pushdown_project):
        kylin_client.create_project(project_name=pushdown_project)

    hive_tables = kylin_client.list_hive_tables(project_name=project)
    if hive_tables is not None:
        for table_info in kylin_client.list_hive_tables(project_name=project):
            if table_info.get('source_type') == 0:
                kylin_client.load_table(project_name=pushdown_project,
                                        tables='{database}.{table}'.format(
                                            database=table_info.get('database'),
                                            table=table_info.get('name')))
    kylin_resp = kylin_client.execute_query(cube_name=cube,
                                            project_name=project,
                                            sql=sql)
    assert kylin_resp.get('isException') is False

    pushdown_resp = kylin_client.execute_query(project_name=pushdown_project, sql=sql)
    assert pushdown_resp.get('isException') is False

    assert query_result_equals(kylin_resp, pushdown_resp)