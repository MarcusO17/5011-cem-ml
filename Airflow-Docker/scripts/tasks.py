import requests
import pprint
import pandas as pd

# getting latest data for current day
def get_daily_data():

    urls = ["https://api.data.gov.my/data-catalogue?id=covid_cases&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_vaxstatus&limit=1",
            "https://api.data.gov.my/data-catalogue?id=covid_cases_age&limit=1"]

    datasets = list()

    for url in urls:
        response_json = requests.get(url=url).json()
        datasets.append(response_json)

    print(datasets)
    return datasets

#combine the datasets into one row
def consolidate_data(ti):

    datasets = ti.xcom_pull(task_ids="get_data")
    combined_datasets = {}
    for dataset in datasets:
        for key,value in dataset[0].items():
            combined_datasets[key] = value

    df = pd.DataFrame([combined_datasets])
    df = df.drop_duplicates()
    pd.set_option('display.max_columns', None)

    print(df)

    return df

