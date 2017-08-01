#coding: utf-8

import sys
import json
import requests
from functools import reduce
from openpyxl import load_workbook

CREATE_SENSOR_API = "http://119.254.211.60:8000/api/1.0.0/sensors/"


def create_sensor(device_id, name=""):
    headers = {'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJvcmlnX2lhdCI6MTUwMTE0MjM5NCwiZXhwIjoxNTAzNzM0Mzk0LCJ1c2VyX2lkIjoieHZtanNKOXFnbUxjQ2ZqWm5RYnR5NiIsImVtYWlsIjoieXpnOTYzQGdtYWlsLmNvbSIsInVzZXJuYW1lIjoieXpnOTYzQGdtYWlsLmNvbSJ9.IiRN75jXdXAoYZ8kbPgIT7b9MWbFWGR9hDRKiHz6Nh0'}
    json_para = {"device": device_id, "data_source": "external", "data_type": -1, "name": name}
    rsp = requests.post(CREATE_SENSOR_API, headers=headers, json=json_para)
    return json.loads(rsp.content).get('id')


key_list = ['Parameter', 'DisplayType', 'Description', 'Unit']

wb = load_workbook(filename=sys.argv[1])
sheets = wb.get_sheet_names()
ws = wb.get_sheet_by_name(sheets[0])

result = [0, 0, 0, 0]
for _ in ws.columns:
    line = [c.value for c in _]
    if line[0] in key_list:
        result[key_list.index(line[0])] = line


json_output = {}
for _ in list(zip(*result))[1:]:
    if _[1] != 'hide':
        sensor_id = create_sensor(name=_[0], device_id=sys.argv[2])
        json_output[_[0]] = {'sensor_id': sensor_id, 'unit': _[3], 'data_type': 0}
        print("|{sensor}|{name}|{unit}|{desc}|{display_type}|".format(sensor=sensor_id, name=_[0], desc=_[2], display_type=_[1], unit=_[3]))

print(json.dumps(json_output))
# print(json.dumps(dict(map(lambda _: (_[0], {'sensor_id': '', 'unit': _[3], 'data_type': 0}),
# filter(lambda x: x[1] != 'hide', list(zip(*result))[1:])))))

