import datetime

# make timestamp column 'Time' start from Unix epoch 1.1.1970 00:00
def NormalizeTime(df):
    epoch = datetime.datetime.utcfromtimestamp(0)
    diff = df['Time'][0] - epoch
    for i in range(len(df.index)):
        df['Time'][i] = df['Time'][i] - diff

# add column containing elapsed time as a fraction of total duration
def CreateElapsedTimeColumn(df):
    t_start = df['Time'].iloc[0]
    t_end = df['Time'].iloc[-1]
    t_tot = t_end - t_start
    elapsed = []
    for _, row in df.iterrows():
        t = row['Time'] - t_start
        elapsed.append(t / t_tot)
    df['Elapsed Time'] = elapsed
    return t_start.to_pydatetime(), t_end.to_pydatetime()