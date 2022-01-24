import logging
import time

import requests
from retrying import retry


logger = logging.getLogger(__name__)


def _result(exception):
    return isinstance(exception, requests.exceptions.ConnectTimeout)


class BasicHttpClient:
    _host = None
    _port = None

    _headers = {}

    _auth = ('ADMIN', 'KYLIN')

    _inner_session = requests.Session()

    def __init__(self, host, port):
        if not host or not port:
            raise ValueError('init http client failed')

        self._host = host
        self._port = port

    def token(self, token):
        self._headers['Authorization'] = 'Basic {token}'.format(token=token)

    def auth(self, username, password):
        self._auth = (username, password)

    def header(self, name, value):
        self._headers[name] = value

    def headers(self, headers):
        self._headers = headers

    def _request(self, method, url, params=None, data=None, json=None,  # pylint: disable=too-many-arguments
                 files=None, headers=None, stream=False, to_json=True, inner_session=False, timeout=60, content=False,
                 raw_response=False):
        if inner_session:
            return self._request_with_session(self._inner_session, method, url,
                                              params=params,
                                              data=data,
                                              json=json,
                                              files=files,
                                              headers=headers,
                                              stream=stream,
                                              to_json=to_json,
                                              timeout=timeout,
                                              content=content,
                                              raw_response=raw_response
                                              )
        with requests.Session() as session:
            session.auth = self._auth
            return self._request_with_session(session, method, url,
                                              params=params,
                                              data=data,
                                              json=json,
                                              files=files,
                                              headers=headers,
                                              stream=stream,
                                              to_json=to_json,
                                              timeout=timeout,
                                              content=content,
                                              raw_response=raw_response
                                              )

    @retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=200, retry_on_exception=_result)
    def _request_with_session(self, session, method, url, params=None, data=None,  # pylint: disable=too-many-arguments
                              json=None, files=None, headers=None, stream=False,
                              to_json=True, timeout=60, content=False, raw_response=False):
        if headers is None:
            headers = self._headers
        resp = session.request(method, url,
                               params=params,
                               data=data,
                               json=json,
                               files=files,
                               headers=headers,
                               stream=stream,
                               timeout=timeout,
                               verify=False
                               )

        try:
            if raw_response:
                return resp
            if stream:
                return resp.raw
            if not resp.content:
                return None
            if content:
                return resp.content
            if to_json:
                data = resp.json()
                resp.raise_for_status()
                return data.get('data', data)
            return resp.text
        except requests.exceptions.ReadTimeout as timeout_error:
            logger.error(timeout_error)
            time.sleep(60)
        except requests.HTTPError as http_error:
            err_msg = f"{str(http_error)} [return code: {data.get('code', '')}]-[{data.get('msg', '')}]\n" \
                      f"{data.get('stacktrace', '')}"
            logger.error(err_msg)
            raise requests.HTTPError(err_msg, request=http_error.request, response=http_error.response, )
        except Exception as error:
            logger.error(str(error))
            raise error
