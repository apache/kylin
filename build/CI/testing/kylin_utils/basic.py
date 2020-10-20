import logging
import requests


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
                 files=None, headers=None, stream=False, to_json=True, inner_session=False, timeout=60):
        if inner_session:
            return self._request_with_session(self._inner_session, method, url,
                                              params=params,
                                              data=data,
                                              json=json,
                                              files=files,
                                              headers=headers,
                                              stream=stream,
                                              to_json=to_json,
                                              timeout=timeout)
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
                                              timeout=timeout)

    def _request_with_session(self, session, method, url, params=None, data=None,  # pylint: disable=too-many-arguments
                              json=None, files=None, headers=None, stream=False, to_json=True, timeout=60):
        if headers is None:
            headers = self._headers
        resp = session.request(method, url,
                               params=params,
                               data=data,
                               json=json,
                               files=files,
                               headers=headers,
                               stream=stream,
                               timeout=timeout
                               )

        try:
            if stream:
                return resp.raw
            if not resp.content:
                return None

            if to_json:
                data = resp.json()
                resp.raise_for_status()
                return data
            return resp.text
        except requests.HTTPError as http_error:
            err_msg = f"{str(http_error)} [{data.get('msg', '')}]\n" \
                      f"{data.get('stacktrace', '')}"
            logging.error(err_msg)
            raise requests.HTTPError(err_msg, request=http_error.request, response=http_error.response, )
        except Exception as error:
            logging.error(str(error))
            raise error
