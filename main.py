import csv
import json
import requests
from pprint import pprint
import datetime
import time
import sys

HOST_FILE = 'hyg-data.csv'
# VM_URL = 'https://vminsert-any-qa.mailcore.be-intg-iz1-bs.poinfra.server.lan/insert/0/prometheus/api/v1/import'
#VM_URL = 'http://172.17.0.1:8428/prometheus/api/v1/import'
VM_URL = 'http://10.20.1.95:8428/prometheus/api/v1/import'
METRIC_TYPE = ['temperature', 'humidity']


def get_Timestamp(humanDate):
    if "\ufeff" in humanDate:
        timestamp = None
    else:
        split_date, split_time = humanDate.split(' ')
        year, month, day = split_date.split('-')
        hour, minute, secound = split_time.split(':')
        dt = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(secound))
        ts = time.mktime(dt.timetuple())
        timestamp = int(ts) * 1000
    return timestamp


with open(HOST_FILE) as csvfile:
    item_list = csv.reader(csvfile, delimiter=',')

    timestamps = []
    values = []

    for mtype in METRIC_TYPE:
        for row in item_list:
            if mtype == 'temperature':
                i = 1
            elif mtype == 'humidity':
                i = 2

            if get_Timestamp(row[0]) is None:
                continue
            else:
                timestamps.append(get_Timestamp(row[0]))
                values.append(float(row[i]))

        with open('vmsample.json', 'r') as json_file:
            data_layout = json.load(json_file)

            data_layout["metric"]["type"] = mtype

            if get_Timestamp(row[0]) is None:
                continue
            else:
                data_layout["timestamps"] = timestamps
                data_layout["values"] = values

        pprint(data_layout)

        result = requests.post(VM_URL, json=data_layout, verify=False)
        pprint(result.status_code)
