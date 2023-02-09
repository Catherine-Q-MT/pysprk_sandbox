import json
from typing import List

import requests
from pathlib import Path

URL = 'https://api.cqc.org.uk/public/v1/locations'
data_dir = Path('./data')


def get_location_data_as_json():
    with open(data_dir / 'data.json', 'w+') as f:
        data = requests.get(URL)
        location_data = data.json()
        f.write(json.dumps(location_data.get('locations')))


def get_location_specific_data(location_list: List[str]):
    with open(data_dir / 'location_data.json', 'w+') as f:
        location_data_list = []
        for location in location_list:
            data = requests.get(f'https://api.cqc.org.uk/public/v1/locations/{location}?partnerCode=OpenAnswers')
            location_data_list.append(data.json())
        f.write(json.dumps(location_data_list))



def show_data_summary():
    req = requests.get(URL)
    print(req.status_code)
    print(req.headers)
    print(req.text)
