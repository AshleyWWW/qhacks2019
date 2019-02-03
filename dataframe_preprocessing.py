# -*- coding: utf-8 -*-
"""
Spyder Editor

"""
import datetime
import pandas as pd
from datetime import datetime 

def read_csv(filename, values):
    df = pd.read_csv(filename)
    return df[values]    

def elim_rows(df, label, asset):
    #elimates rows containing missing values
    #look for 'unknown' in all columns
    #look for NaN and elim.
    for col in list(df):
        df = df[df[col].notna()]
    df = df.loc[~df[asset].str.contains('Unknown', regex=False)]
    df = df.loc[~df[label] != 2]
    return df

def shift(df, col):
    #columns of shifted values
    
    df[col+'Past1']=df[col].shift(1)
    df[col+'Past2']=df[col].shift(2)
    df[col+'Past3']=df[col].shift(3)
    return df

def time_to_date(df, time):
    for i in range(len(df.index)):
        df[time].iloc[i] = df[time].iloc[i][:-6]
    df[time] = pd.to_datetime(df[time]).dt.date
    df[time] = df[time].astype('datetime64[ns]')
    return df

def mergedf(df1, df2, col):
    #merge dataframes based on time column
    return pd.merge(df1, df2, on=col)

def combine_strings(df, time, asset, headline):
    return df.groupby([time,asset],as_index=False).agg(lambda x : ' '.join(x))

def assetsort(df, asset, time):
    df= df.sort_values([asset, time])
    return df.reset_index(drop=True)
    
def time_to_string(ns):
    dt = datetime.fromtimestamp(ns // 1000000000)
    s = dt.strftime('%Y-%m-%d')
    return s

def string_to_ns(time):
    dt_obj = datetime.strptime(time, '%Y-%m-%d')
    nanosec = dt_obj.timestamp() * 1000000000
    return int(nanosec)

def interp(df, timecol, asset, val):
    first = string_to_ns(str(df[timecol].iloc[0]))
    end = string_to_ns(str(df[timecol].iloc[-1]))
    time = list(range(first, end+86400000000000, 86400000000000))
    new_df = pd.DataFrame(index=range(len(time)), columns = list(df))
    for i, t in enumerate(time):
        time[i] = time_to_string(t)
    new_df[timecol] = time
    new_df['assetName'] = asset
    for i in range(len(df.index)):
        value = str(df[timecol].iloc[i])
        rowIndex = new_df[new_df[timecol]== value].index[0]
        new_df.loc[rowIndex,asset] = df[val].iloc[i]
    return new_df

def shift_asset_sensitive(df, assetName, value, timecol,chunksize):
    #df is sorted by asset and time.
    assets = list(pd.unique(df[assetName]))
    bigdata=None
    for asset in assets:
        new_df = df[ df[assetName] == asset]
        if (new_df.shape[0]) > chunksize:
            data1 = interpolate_days(new_df, timecol, asset, assetName)
            if bigdata is None:
                bigdata = shift(data1, value)[3:]
            else:
                data1 = shift(data1, value)[3:]
                bigdata = bigdata.append(data1, ignore_index=True)
        else:
            continue
    return bigdata

def create_label(dff, current, previous, label):
    def mov_percent(prev, cur):
        return (cur - prev) / (prev + 10e-10)
    dff = dff.assign(label=2) # create a new col
    for i in range(len(dff.index)):
        if mov_percent(dff[previous].iloc[i], dff[current].iloc[i]) < -0.005:
            dff[label].iloc[i] = 0
        elif mov_percent(dff[previous].iloc[i], dff[current].iloc[i]) > 0.0055:
            dff[label].iloc[i] = 1
    return dff

def interpolate_days(dff, dates, asset, assetName):
    span = pd.date_range(dff[dates].min(), dff[dates].max(), freq="D")
    expanded = pd.DataFrame(index=span.copy()).interpolate("time", columns=dates)
    expanded[dates] = expanded.index
    df = pd.merge(dff, expanded, how="outer", on=[dates], sort=True )
#    print(df)
    df[assetName] = asset
    return df
    
def main():
    #read csv files
    news_dataframe = read_csv('news_sample.csv', 'time headline assetName'.split())
    mkt_dataframe = read_csv('mkt_sample.csv', 'time close assetName'.split())
    
    #eliminate time from date time
    news_dataframe = time_to_date(news_dataframe, 'time')
    mkt_dataframe = time_to_date(mkt_dataframe, 'time')

    #concatenate strings and sort by asset and time
    news_dataframe = combine_strings(news_dataframe, 'time', 'assetName', 'headline')
    news_dataframe = assetsort(news_dataframe, 'assetName', 'time')
    mkt_dataframe = assetsort(mkt_dataframe, 'assetName', 'time')

    #interpolate time column in each dataframe 
    newsdf = shift_asset_sensitive(news_dataframe,'assetName','headline','time',3)
    mktdf = shift_asset_sensitive(mkt_dataframe,'assetName','close','time',1)
    
    mkt_dataframe = create_label(mktdf,'close', 'closePast1', 'label')
    
    #merge file
    if newsdf is not None and mkt_dataframe is not None:
        merged = mergedf(newsdf, mkt_dataframe, ['time', 'assetName'])
        print(merged[['headlinePast1', 'headlinePast2']])
        #remove nan rows
        merged = elim_rows(merged, "label", "assetName")
        del merged['closePast1']
        del merged['closePast2']
        del merged['closePast3']
        del merged['headline']
        del merged['close']
        merged.to_csv('sampleTest.csv', index=False)
    else:
        raise TypeError("Insufficient market or news data")

if __name__=="__main__":
    main()