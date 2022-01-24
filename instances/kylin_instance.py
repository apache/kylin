from client import lightning


class KylinInstance:

    def __init__(self, **kwargs):
        self._host = kwargs['host']
        self._port = kwargs['port']
        self.client = lightning.connect(host=self._host, port=self._port)
