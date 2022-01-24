import logging
import time

from client.basic import BasicHttpClient

logger = logging.getLogger(__name__)


class LightningHttpClient(BasicHttpClient):
    _base_url = 'http://{host}:{port}'

    def __init__(self, host, port):
        super().__init__(host, port)
        self._headers = {
            'Accept': 'application/json',
            'Accept-Language': 'en-US',
            'Content-Type': 'application/json;charset=utf-8'
        }
        self._base_url = self._base_url.format(host=self._host, port=self._port)

    def set_headers(self, key, value=None):
        if value is not None:
            self._headers[key] = value
        else:
            if key in self._headers.keys():
                self._headers.pop(key)

    def set_base_url(self, url):
        self._base_url = url

    def await_kylin_start(self, check_action=None, timeout=600, check_times=30, **kwargs):
        start_time = time.time()
        already_check_times = 0
        while time.time() - start_time < timeout:
            try:
                while already_check_times < check_times:
                    res = check_action(**kwargs)
                    assert res
                    already_check_times = already_check_times + 1
                    logger.info(f'Already check {already_check_times} times')
                    time.sleep(1)
                return True
            except Exception as e:
                logger.debug('Kylin can not access now {}, wait 10s'.format(e))
                already_check_times = 0
                time.sleep(10)
        logger.debug('Kylin can not access after {}s'.format(timeout))
        return False

    def login(self, username, password, user_session=False):
        """
        use username and password login
        :param username: string, target group name
        :param password: array, the users add to group
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/kylin/api/user/authentication'
        self.auth(username, password)
        resp = self._request('POST', url, inner_session=user_session)
        return resp

    def get_session(self):
        return self._inner_session

    def check_login_state(self):
        url = '/kylin/api/user/authentication'
        resp = self._request('GET', url, inner_session=False)
        return resp

    def execute_query(self, project, sql, offset=0, limit=500, accept_partial=False, user_session=False, timeout=360):
        """
        :param project: string, the name of project
        :param sql: query sql
        :param offset: offset of returned result
        :param limit: limit of returned result
        :param timeout: session timeout time
        :return:
        """
        url = '/kylin/api/query'
        payload = {
            'limit': limit,
            'offset': offset,
            'project': project,
            'acceptPartial': accept_partial,
            'sql': sql,
        }
        resp = self._request('POST', url, json=payload, inner_session=user_session, timeout=timeout)
        return resp

    def _request(self, method, url, **kwargs):  # pylint: disable=arguments-differ
        return super()._request(method, self._base_url + url, **kwargs)


def connect(**conf):
    _host = conf.get('host')
    _port = conf.get('port')

    return LightningHttpClient(_host, _port)
