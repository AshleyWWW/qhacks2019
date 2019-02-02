import pandas, datetime

def create_label(dff, current, previous, label):
    dff = dff.assign(label=2) # create a new col
    for i in range(len(dff.index)):
        if mov_percent(dff[previous].iloc[i], dff[current].iloc[i]) < -0.005:
            dff[label].iloc[i] = 0
        elif mov_percent(dff[previous].iloc[i], dff[current].iloc[i]) > 0.0055:
            dff[label].iloc[i] = 1
    return dff

def mov_percent(prev, cur):
    return (cur - prev) / (prev + 10e-10)

def interpolate_days(dff, dates):
    span = pandas.date_range(dff[dates].min(), dff[dates].max(), freq="D")
    expanded = pandas.DataFrame(index=span.copy()).interpolate("time", columns=dates)
    expanded[dates] = expanded.index
    return pandas.merge(dff, expanded, how="outer", on=[dates])

if __name__ == "__main__":
    df = pandas.DataFrame({"time": [datetime.datetime(2000, 1, 1),
                                    datetime.datetime(2000, 1, 3),
                                    datetime.datetime(2000, 2, 1),
                                    datetime.datetime(2000, 1, 1),
                                    datetime.datetime(2000, 1, 3),
                                    datetime.datetime(2000, 2, 1)],
                           "assetCode": ["a", "a", "a", "b", "b", "b"],
                           "current":[1,2,3,4,5,6],"past1":[0,9,8,7,6,6],
                           "past2":[3,4,5,6,7,8]})
    print(interpolate_days(df, "time"))