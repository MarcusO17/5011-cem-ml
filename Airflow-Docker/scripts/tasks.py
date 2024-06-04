import pandas as pd
import datetime

# get data for last week
def get_weekly_data():
    # put the datasets that can be filtered the same way together
    # some are using dates, some are counted in weeks, some are in date ranges

    url_cluster = "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/clusters.csv"  # highest date announce

    urls_week = ["https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_age.csv",
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_age.csv"]

    urls_date = ["https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_malaysia.csv",
                 # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/cases_state.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_malaysia.csv",
                 # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/deaths_state.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv",  # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv",
                 # date
                 "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_state.csv"]  # date

    week_all_datasets = list()
    date_all_datasets = list()

    weekly_datasets = list()  # [cases_death, death_age, cases_malaysia, cases_states, deaths_malaysia, deaths_state, hospital, icu, tests_malaysia, tests_state, clusters]

    list_of_dates = get_previous_week_dates()

    for url in urls_week:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        week_all_datasets.append(df)

    for dataset in week_all_datasets:

        week = dataset["week"]

        list_of_week = list(map(lambda i: i[1:], week))

        for i in range(0, len(list_of_week)):
            list_of_week[i] = int(list_of_week[i])

        result = dataset[dataset["week"] == "w" + str(max(list_of_week))]

        weekly_datasets.append(result)

    for url in urls_date:
        response_json = pd.read_csv(url)

        df = pd.DataFrame(response_json)
        date_all_datasets.append(df)

    for dataset in date_all_datasets:
        dataset["date"] = pd.to_datetime(dataset["date"])
        dataset = dataset[dataset["date"].isin(list_of_dates)]

        weekly_datasets.append(dataset)

        print(dataset)

    response_json = pd.read_csv(url_cluster)

    dataset = pd.DataFrame(response_json)

    dataset["date_announced"] = pd.to_datetime(dataset["date_announced"])

    dataset = dataset[dataset["date_announced"].isin(list_of_dates)]

    weekly_datasets.append(dataset)

    print(dataset)

    return weekly_datasets


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


