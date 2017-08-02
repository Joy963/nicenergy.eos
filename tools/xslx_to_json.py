# coding: utf-8
import sys
import json
import requests
from openpyxl import load_workbook

device_id = sys.argv[2]
CREATE_SENSOR_API = "http://119.254.211.60:8000/api/1.0.0/sensors/"

unit_map = {
    "℃": {"data_type": 0, "desc": "摄氏度(˚C)"},
    "GeoPoint": {"data_type": 1, "desc": "地理位置(BDPoint)"},
    "V": {"data_type": 3, "desc": "电压, Volt(V)"},
    "A": {"data_type": 4, "desc": "电流, 安培(A)"},
    "m/s": {"data_type": 5, "desc": "速度(m/s)"},
    "Diagnostic": {"data_type": 6, "desc": "盒子诊断数据(Object)"},
    "Log": {"data_type": 7, "desc": "日志(Object)"},
    "kPa": {"data_type": 8, "desc": "压力，千帕(kPa)"},
    "m3/h": {"data_type": 9, "desc": "流量(m3/h)"},
    "mm": {"data_type": 10, "desc": "液位(mm)"},
    "%RH": {"data_type": 11, "desc": "相对湿度(%RH)"},
    "%": {"data_type": 12, "desc": "通用百分比(%)"},
    "kWh": {"data_type": 13, "desc": "电量(kWh)"},
    "h": {"data_type": 14, "desc": "时长(h)"},
    "W": {"data_type": 15, "desc": "直流功率(W)"},
    "kW": {"data_type": 16, "desc": "有功功率(kW)"},
    "kVar": {"data_type": 17, "desc": "无功功率(kVar)"},
    "Hz": {"data_type": 18, "desc": "频率(Hz)"},
    "Mpa": {"data_type": 19, "desc": "压力(Mpa)"},
    "kL/h": {"data_type": 20, "desc": "系统产氢率(kL/h)"},
    "kVA": {"data_type": 21, "desc": "视在功率(kVA)"},
    "m/min": {"data_type": 22, "desc": "负载速度(m/min)"},
    "min": {"data_type": 23, "desc": "时长(min)"},
    "L/min": {"data_type": 24, "desc": "流量(L/min)"},
    "ml": {"data_type": 25, "desc": "体积(ml)"}
}


def get_data_type(unit):
    if not unit:
        return -1
    for k, v in unit_map.items():
        if k.lower() == unit.lower():
            return v.get('data_type')
    return -1


def create_sensor(name=""):
    headers = {'Authorization': 'JWT eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJvcmlnX2lhdCI6MTUwMTE0MjM5NCwiZXhwIjoxNTAzNzM0Mzk0LCJ1c2VyX2lkIjoieHZtanNKOXFnbUxjQ2ZqWm5RYnR5NiIsImVtYWlsIjoieXpnOTYzQGdtYWlsLmNvbSIsInVzZXJuYW1lIjoieXpnOTYzQGdtYWlsLmNvbSJ9.IiRN75jXdXAoYZ8kbPgIT7b9MWbFWGR9hDRKiHz6Nh0'}
    json_para = {"device": device_id, "data_source": "external", "data_type": -1, "name": name}
    rsp = requests.post(CREATE_SENSOR_API, headers=headers, json=json_para)
    return json.loads(rsp.content).get('id')

# import xlsx
key_list = ['Parameter', 'DisplayType', 'Description', 'Unit']
wb = load_workbook(filename=sys.argv[1])
sheets = wb.get_sheet_names()
ws = wb.get_sheet_by_name(sheets[0])

# sort
result = [0, 0, 0, 0]
for _ in ws.columns:
    line = [c.value for c in _]
    if line[0] in key_list:
        result[key_list.index(line[0])] = line


# create sensor by request api and then generate markdown file and json map file
json_output = {}
try:
    content_json_output = open('output.json', 'w+').read()
except ValueError:
    content_json_output = {}

with open('output.md', 'w') as f_md, open('output.json', 'w') as f_json:
    f_md.writelines([
        "# PEM: %s\n\n" % device_id,
        "|Sensor|Name|Unit|Desc|DisplayType|\n"
        "|----|----|----|----|----|\n"
    ])
    for _ in list(zip(*result))[1:]:
        if _[1] != 'hide' and _[0] not in content_json_output:
            # sensor_id = create_sensor(name=_[0])
            sensor_id = ''
            json_output[_[0]] = {'sensor_id': sensor_id, 'unit': _[3], 'data_type': get_data_type(_[3])}
            line = "|{sensor}|{name}|{unit}|{desc}|{display_type}|\n".format(
                sensor=sensor_id, name=_[0], desc=_[2], display_type=_[1], unit=_[3])
            f_md.writelines([line])
    f_json.write(json.dumps({'KEY': json_output}))

