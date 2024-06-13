import datetime

def date_range(start_date, end_date, monthly=False):
    start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    dates = []

    while start_dt <= end_dt:
        dates.append(start_dt.strftime("%Y-%m-%d"))
        start_dt += datetime.timedelta(days=1)

    if monthly:
        return [i for i in dates if i.endswith("01")]
           
    return dates

date_range("2024-01-01", "2024-08-01", monthly=True)