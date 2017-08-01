import json


def get_unit_map():
    with open('../unit_map.json') as f:
        try:
            unit_map = json.loads(f.read())
        except ValueError as e:
            print(e)
            return {}
    return unit_map


def get_data_type(unit_map, unit):
    if not isinstance(unit_map, dict):
        return -1
    for k, v in unit_map.items():
        if k.lower() == unit.lower():
            return v.get('data_type')
    return -1


def conv_sensor_map():
    unit_map = get_unit_map()
    with open('../sensor_map_first.json') as f_r, open('../sensor_map.json', 'a+') as f_w:
        try:
            sensor_map = json.loads(f_r.read())
        except ValueError as e:
            print(e)
            return

        output = {}
        for _, __ in sensor_map.items():
            print(_)
            tmp = {}
            for k, v in __.items():
                unit = v.get('unit')
                data_type = get_data_type(unit_map, unit) if unit else -1
                tmp[k] = dict(v, **{'data_type': data_type})
            output[_] = tmp
        f_w.write(json.dumps(output))


if __name__ == '__main__':
    conv_sensor_map()

