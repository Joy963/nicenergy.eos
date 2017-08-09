#!/usr/bin/env python
# coding: utf-8
# Author: `mageia`,
# Email: ``,
# Date: `09/08/2017 09:31`
# Description: ''
import json

with open('../sensor_map_old.json') as f_old, open('../sensor_map.json') as f_new:
    old = json.loads(f_old.read())
    new = json.loads(f_new.read())

    old_top_keys = set(old.keys())
    new_top_keys = set(new.keys())

    for k in old_top_keys:
        old_v = old.get(k)
        new_v = new.get(k)
        old_second_keys = set(old_v.keys())
        new_second_keys = set(new_v.keys())

        if old_second_keys == new_second_keys:
            print(k, ' PASS')
        else:
            print(k)
            print('old - new: ', old_second_keys - new_second_keys)
            print('new - old: ', new_second_keys - old_second_keys)

