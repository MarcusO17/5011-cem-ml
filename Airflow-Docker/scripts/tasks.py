import requests
import pprint
import pandas as pd

def get_daily_data():

    urls = ["https://api.data.gov.my/data-catalogue?id=covid_cases&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_vaxstatus&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_age&limit=1"]

    datasets = list()

    for url in urls:
        response_json = requests.get(url=url).json()
        datasets.append(response_json)

    pprint.pprint(datasets)


