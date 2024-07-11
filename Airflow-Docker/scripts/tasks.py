import pandas as pd
import datetime
from airflow.models import Variable
import numpy as np

# get data for last week
def get_weekly_epidemic_data():
    # make a dataframe for state and another for national level
    # not sure how to combine them so its better to separate them

    urls_state = ["https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_state.csv",
                  "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_state.csv",# date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_state.csv"]  # date

    urls_national = ["https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_malaysia.csv",
                     "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_malaysia.csv",
                     "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv"]

    list_of_dates = get_previous_week_dates()

    datasets_state = list()
    datasets_national = list()

    weekly_datasets_state = list()  # [cases_states, deaths_state, hospital, icu, tests_state]
    weekly_datasets_national = list() # cases_malaysia, deaths_malaysia, tests_malaysia

    for url in urls_state:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        datasets_state.append(df)

    for dataset in datasets_state:
        dataset["date"] = pd.to_datetime(dataset["date"])
        dataset = dataset[dataset["date"].isin(list_of_dates)]

        weekly_datasets_state.append(dataset)

    for url in urls_national:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        datasets_national.append(df)

    for dataset in datasets_national:
        dataset["date"] = pd.to_datetime(dataset["date"])
        dataset = dataset[dataset["date"].isin(list_of_dates)]

        weekly_datasets_national.append(dataset)

    return weekly_datasets_state, weekly_datasets_national

# dropping columns for epidemic tables
def data_cleaning(ti):
    weekly_datasets_state, weekly_datasets_national = ti.xcom_pull(task_ids="epidemic_data_preprocessing")
    epidemic_state_columns = ["cases_child", "cases_adolescent", "cases_adult", "cases_elderly", "deaths_tat"]
    epidemic_national_columns = ["cluster_import", "cluster_religious", "cluster_community", "cluster_highRisk", "cluster_education", "cluster_detentionCentre", "cluster_workplace", "deaths_tat","cases_child", "cases_adolescent", "cases_adult", "cases_elderly"]

    for col in epidemic_state_columns:
        weekly_datasets_state = weekly_datasets_state.drop(columns=[col])

    for col in epidemic_national_columns:
        weekly_datasets_national = weekly_datasets_national.drop(columns=[col])

    return weekly_datasets_state, weekly_datasets_national
# combining multiple dataframes
def consolidate_epidemic_data(ti):

    weekly_datasets_state, weekly_datasets_national = ti.xcom_pull(task_ids="get_epidemic_data")
    combined_epidemic_state = weekly_datasets_state[0]

    for dataset in weekly_datasets_state[1:]:
        combined_epidemic_state = pd.merge(combined_epidemic_state, dataset, on=["date", "state"], how="outer")

    combined_epidemic_national = weekly_datasets_national[0]

    for dataset in weekly_datasets_national[1:]:
        combined_epidemic_national = pd.merge(combined_epidemic_national, dataset, on="date", how="outer")

    combined_epidemic_state["date"] = combined_epidemic_state["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    combined_epidemic_national["date"] = combined_epidemic_national["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    combined_epidemic_state = combined_epidemic_state.rename(columns={"rtk-ag": "rtk_ag"})
    combined_epidemic_national = combined_epidemic_national.rename(columns={"rtk-ag": "rtk_ag"})

    missing_testing_dates_state = combined_epidemic_state.loc[combined_epidemic_state["rtk_ag"].isnull() | combined_epidemic_state["pcr"].isnull(), ["date", "state"]]
    missing_testing_dates_national = combined_epidemic_national.loc[combined_epidemic_national["rtk_ag"].isnull() | combined_epidemic_national["pcr"].isnull(), "date"]

    missing_testing_json = missing_testing_dates_state.to_json()
    missing_national_json = missing_testing_dates_national.to_json()

    # setting missing testing values so it can be inserted next week
    Variable.set("missing_testing_dates_state", missing_testing_json)
    Variable.set("missing_testing_dates_national", missing_national_json)

    epidemic_national_col = ["deaths_new", "deaths_bid", "deaths_new_dod", "deaths_bid_dod", "deaths_unvax", "deaths_pvax",
                             "deaths_fvax", "deaths_boost", "cases_new", "cases_import", "cases_recovered", "cases_active",
                             "cases_cluster", "cases_unvax", "cases_pvax", "cases_fvax", "cases_boost", "cases_child",
                             "cases_adolescent", "cases_adult", "cases_elderly", "cases_0_4", "cases_5_11", "cases_12_17",
                             "cases_18_29", "cases_30_39", "cases_40_49", "cases_50_59", "cases_60_69", "cases_70_79",
                             "cases_80", "rtk_ag", "pcr"]
    placeholder = -1

    # changing any NaN values into placeholder value
    for col in epidemic_national_col:
        print(col)

        combined_epidemic_national[col] = combined_epidemic_national[col].fillna(placeholder)
        combined_epidemic_state[col] = combined_epidemic_state[col].fillna(placeholder)

        combined_epidemic_national[col] = combined_epidemic_national[col].astype(int)
        combined_epidemic_state[col] = combined_epidemic_state[col].astype(int)

        #combined_epidemic_national[col] = combined_epidemic_national[col].replace(placeholder, None)
        #combined_epidemic_state[col] = combined_epidemic_state[col].replace(placeholder, None)

        print(combined_epidemic_national[col])
        print(combined_epidemic_state[col])

    epidemic_state_col = ["beds", "beds_covid", "beds_noncrit", "admitted_pui", "admitted_covid", "admitted_total",
                          "discharged_pui",
                          "discharged_covid", "discharged_total", "hosp_covid", "hosp_pui", "hosp_noncovid", "beds_icu",
                          "beds_icu_rep",
                          "beds_icu_total", "beds_icu_covid", "vent", "vent_port", "icu_covid", "icu_pui",
                          "icu_noncovid", "vent_covid",
                          "vent_pui", "vent_noncovid", "vent_used", "vent_port_used"]

    for col in epidemic_state_col:
        combined_epidemic_state[col] = combined_epidemic_state[col].fillna(placeholder)
        combined_epidemic_state[col] = combined_epidemic_state[col].astype(int)
        print(type(combined_epidemic_state[col]))

    return combined_epidemic_state, combined_epidemic_national

# use to update test data inconsistent update schedule
def update_missing_testing_data(client):
    try:
        missing_testing_dates_state = Variable.get("missing_testing_dates_state")
        missing_testing_dates_national = Variable.get("missing_testing_dates_national")

        # convert into dataframe
        missing_testing_dates_state = pd.read_json(missing_testing_dates_state)
        missing_testing_dates_national = pd.read_json(missing_testing_dates_national)
    except Exception as e:
        print(e)
        return

    if missing_testing_dates_state is None or missing_testing_dates_national is None:
        return

    url_testing_state = "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_state.csv"
    url_testing_national = "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv"

    df_testing_state = pd.read_csv(url_testing_state)
    df_testing_state["date"] = pd.to_datetime(df_testing_state["date"])
    # looping to find a match with the date
    for index, row in missing_testing_dates_state.iterrows():
        date = row["date"]
        state = row["state"]
        # insert the data according to the state
        date_mask = (df_testing_state["date"] == pd.to_datetime(date)) & (df_testing_state["state"] == state)
        if date_mask.any():
            rows = df_testing_state[date_mask]
            for index, row in rows.iterrows():
                print(index, row)
                response = client.table("state_epidemic").upsert(
                    [{
                        "date": row["date"],
                        "state": row["state"],
                        "rtk-ag": row["rtk-ag"],
                        "pcr": row["pcr"],
                    }]
                ).execute()
                print(response)

    df_testing_national = pd.read_csv(url_testing_national)
    df_testing_national["date"] = pd.to_datetime(df_testing_national["date"])
    for date in missing_testing_dates_national:
        date_mask = df_testing_national["date"] == pd.to_datetime(date)
        # same process except you skip the state part, if one true then continue
        if date_mask.any():
            rows = df_testing_national[date_mask]
            for index, row in rows.iterrows():
                print(index, row)
                response = client.table("malaysia_epidemic").upsert(
                    [{
                        "date": row["date"],
                        "rtk-ag": row["rtk-ag"],
                        "pcr": row["pcr"],
                    }]
                ).execute()
                print(response)

def get_weekly_vaccination_data():
    # make a dataframe for state and another for national level
    # not sure how to combine them so its better to separate them

    urls_state = ["https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/vaccination/vax_state.csv"]

    urls_national = ["https://raw.githubusercontent.com/CITF-Malaysia/citf-public/main/vaccination/vax_malaysia.csv"]

    list_of_dates = get_previous_week_dates()

    datasets_state = list()
    datasets_national = list()

    weekly_datasets_state = list()  # [cases_states, deaths_state, hospital, icu, tests_state]
    weekly_datasets_national = list()  # cases_malaysia, deaths_malaysia, tests_malaysia

    for url in urls_state:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        datasets_state.append(df)

    for dataset in datasets_state:
        dataset["date"] = pd.to_datetime(dataset["date"])
        dataset = dataset[dataset["date"].isin(list_of_dates)]

        weekly_datasets_state.append(dataset)

    for url in urls_national:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        datasets_national.append(df)

    for dataset in datasets_national:
        dataset["date"] = pd.to_datetime(dataset["date"])
        dataset = dataset[dataset["date"].isin(list_of_dates)]

        weekly_datasets_national.append(dataset)

    return weekly_datasets_state, weekly_datasets_national


# combining multiple dataframes
def consolidate_vaccination_data(ti):
    weekly_datasets_state, weekly_datasets_national = ti.xcom_pull(task_ids="get_vaccination_data")
    combined_df_state = weekly_datasets_state
    combined_df_national = weekly_datasets_national

    combined_df_state[0]["date"] = combined_df_state[0]["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    combined_df_national[0]["date"] = combined_df_national[0]["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    combined_df_state[0]["state"] = combined_df_state[0]["state"].astype(str)

    dataframes = [combined_df_state, combined_df_national]

    cols = ["daily_partial","daily_full","daily_booster","daily_booster2","daily","daily_partial_adol","daily_full_adol",
            "daily_booster_adol","daily_booster2_adol","daily_partial_child","daily_full_child","daily_booster_child",
            "daily_booster2_child","cumul_partial","cumul_full","cumul_booster","cumul_booster2","cumul","cumul_partial_adol",
            "cumul_full_adol","cumul_booster_adol","cumul_booster2_adol","cumul_partial_child","cumul_full_child","cumul_booster_child",
            "cumul_booster2_child","pfizer1","pfizer2","pfizer3","pfizer4","sinovac1","sinovac2","sinovac3","sinovac4","astra1",
            "astra2","astra3","astra4","sinopharm1","sinopharm2","sinopharm3","sinopharm4","cansino","cansino3","cansino4",
            "pending1","pending2","pending3","pending4"]

    for df in dataframes:
        for col in cols:
            df[0][col] = df[0][col].astype(int)
            print(type(df[0][col]))

    return combined_df_state, combined_df_national

# get all dates from last week
def get_previous_week_dates():
    today = datetime.date.today()
    start_date = today
    # get sunday 0 = monday, 6 = sunday
    if today.weekday() != 6:
        start_date = today - datetime.timedelta(today.weekday() + 1)

    # start on saturday, then countdown 7 days
    start_date = start_date - datetime.timedelta(1)
    previous_week_dates = []
    # loop and get all the date of the weeks starting from saturday
    for i in range(7):
        previous_week_dates.append(start_date - datetime.timedelta(days=i))

    previous_week_dates = pd.to_datetime(previous_week_dates)

    return previous_week_dates

# loop to turn NaN values into None
def make_json_serializable(records):
    result = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        result.append(cleaned_record)
    return result

# loading data into database
def load_data(client, ti):
    vaccination_df_state, vaccination_df_national = ti.xcom_pull(task_ids="vaccination_data_preprocessing")
    epidemic_df_state, epidemic_df_national = ti.xcom_pull(task_ids="epidemic_data_cleaning")

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    vaccination_df_national[0] = vaccination_df_national[0].to_dict(orient='records')
    vaccination_df_state[0] = vaccination_df_state[0].to_dict(orient='records')
    epidemic_df_state = epidemic_df_state.to_dict(orient='records')
    epidemic_df_national = epidemic_df_national.to_dict(orient='records')

    vaccination_df_national[0] = make_json_serializable(vaccination_df_national[0])
    vaccination_df_state[0] = make_json_serializable(vaccination_df_state[0])
    epidemic_df_state = make_json_serializable(epidemic_df_state)
    epidemic_df_national = make_json_serializable(epidemic_df_national)

    print(vaccination_df_state[0])
    print(epidemic_df_national)
    print(epidemic_df_state)

    # inserting rows into tables
    try:
        data, count = client.table("MalaysiaVaccination").upsert(
            vaccination_df_national[0]).execute()
        print("MalaysiaVaccination", data, count)

        data, count = client.table("StateVaccination").upsert(
            vaccination_df_state[0]).execute()
        print("StateVaccination", data, count)

        data, count = client.table("MalaysiaEpidemic").upsert(epidemic_df_national).execute()
        print("MalaysiaEpidemic", data, count)

        # Upsert epidemic data for state level
        data, count = client.table("StateEpidemic").upsert(epidemic_df_state).execute()
        print("StateEpidemic", data, count)
    except Exception as e:
        print(e)
