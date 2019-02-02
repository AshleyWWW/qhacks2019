import pandas

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

if __name__ == "__main__":
    df = pandas.DataFrame({"time": [1, 2, 3, 1, 2, 3],"assetCode": ["a", "a", "a", "b", "b", "b"],
                           "current":[1,2,3,4,5,6],"past1":[0,9,8,7,6,6],"past2":[3,4,5,6,7,8]})
    print(create_label(df, "current", "past1", "label"))