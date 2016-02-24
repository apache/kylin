# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

import json


class JsonSerializableObj:
    def __init__(self):
        pass

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)
