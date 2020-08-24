import pandas as pd
from datetime import timedelta

def LoadCSV(filename):
    df = pd.read_csv(filename, 
                     sep = ',', 
                     skiprows = 5,
                     na_values = '--',
                     parse_dates = ['Time'])

    with open (filename, "r") as fd:
        next(fd)
        startdate = next(fd).partition(',')[2].strip()
        for i in range(4): next(fd)
        startdate = startdate + 'T' + next(fd).partition(',')[0].strip()

    timeindex = []
    time = pd.to_datetime(startdate)
    for index, row in df.iterrows():
        timeindex.append(time)
        time = time + timedelta(seconds=1)

    df['Time'] = timeindex
    df.set_index('Time', inplace=True)
    return df