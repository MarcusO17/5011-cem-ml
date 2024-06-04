import pandas as pd
import datetime

# get data for last week
def get_weekly_epidemic_data():
    # make a dataframe for state and another for national level
    # not sure how to combine them so its better to separate them

    urls_state = ["https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_state.csv",
                  "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_state.csv",# date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_state.csv"]  # date

    urls_national = {"https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_malaysia.csv",
                     "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_malaysia.csv",
                     "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv"}

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

# combining multiple dataframes
def consolidate_data(ti):
    weekly_datasets_state, weekly_datasets_national = ti.xcom_pull(task_ids="get_data")
    combined_df_state = pd.concat(weekly_datasets_state)
    print(combined_df_state)

    print("=========================================================================")

    combined_df_national = pd.concat(weekly_datasets_national)
    print(combined_df_national)

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



