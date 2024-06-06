import pandas as pd
import datetime
from airflow.models import Variable

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

def data_cleaning(ti):
    weekly_datasets_state, weekly_datasets_national = ti.xcom_pull(task_ids="epidemic_data_preprocessing")
    epidemic_state_columns = ["cases_child", "cases_adolescent", "cases_adult", "cases_elderly", "deaths_tat"]
    epidemic_national_columns = ["cluster_import", "cluster_religious", "cluster_community", "cluster_highRisk", "cluster_education", "cluster_detentionCentre", "cluster_workplace", "deaths_tat"]

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

    missing_testing_dates_state = combined_epidemic_state.loc[combined_epidemic_state["rtk-ag"].isnull() | combined_epidemic_state["pcr"].isnull(), ["date", "state"]]
    missing_testing_dates_national = combined_epidemic_national.loc[combined_epidemic_national["rtk-ag"].isnull() | combined_epidemic_national["pcr"].isnull(), "date"]

    missing_testing_json = missing_testing_dates_state.to_json()
    missing_national_json = missing_testing_dates_national.to_json()

    Variable.set("missing_testing_dates_state", missing_testing_json)
    Variable.set("missing_testing_dates_national", missing_national_json)

    return combined_epidemic_state, combined_epidemic_national

def update_missing_testing_data():
    try:
        missing_testing_dates_state = Variable.get("missing_testing_dates_state")
        missing_testing_dates_national = Variable.get("missing_testing_dates_national")

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
    for index, row in missing_testing_dates_state.iterrows():
        date = row["date"]
        state = row["state"]
        date_mask = (df_testing_state["date"] == pd.to_datetime(date)) & (df_testing_state["state"] == state)
        if date_mask.any():

            print("insert data into db for state")

    df_testing_national = pd.read_csv(url_testing_national)
    df_testing_national["date"] = pd.to_datetime(df_testing_national["date"])
    for date in missing_testing_dates_national:
        date_mask = df_testing_national["date"] == pd.to_datetime(date)
        if date_mask.any():

            print("insert data into db for national")

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
    print(combined_df_state)

    print("=========================================================================")

    combined_df_national = weekly_datasets_national
    print(combined_df_national)

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



