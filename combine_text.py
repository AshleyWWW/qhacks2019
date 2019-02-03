import os
from pathlib import Path
import re
import pickle

import numpy as np
import pandas as pd
import datetime

import preprocessor as tpp
tpp.set_options(tpp.OPT.RESERVED, tpp.OPT.EMOJI, tpp.OPT.SMILEY, tpp.OPT.URL, tpp.OPT.NUMBER)

# We only load 88 tickers not all tickers becasue we delete all tickers not in the 88 ticker symbols
# during pre-processing
#with open("tickers_without_$.pkl","rb") as tickers_pickle:
#    all_tickers = pickle.load(tickers_pickle)

def replaceMultiStopMark(text):
    """ Replaces repetitions of stop marks """
    text = re.sub(r"(\.)\1+", ' . ', text)
    return text


def replaceMultiExclamationMark(text):
    """ Replaces repetitions of exlamation marks """
    text = re.sub(r"(\!)\1+", ' ! ', text)
    return text

def replaceMultiDotMark(text):
    """ Replaces repetitions of exlamation marks """
    text = re.sub(r"(\…)\1+", '', text)
    return text

def remove_symbols(line):
    text = re.sub("[+\/_%^*(+\">]+|[+——！，。？、~@#￥%……*（）•►]+|[\s\.\!,$\'&]{2+}|(\[ video \])", "", line)
    text = re.sub("( ,){2,}", " ,", text) # remove multiple commas
    return text

def self_clean(text_list):
    # text_list is a list of strings
    word_array = np.array(text_list)
    indices = np.where(word_array == '$')[0]
    new_list = []
    idx = 0
    while(idx < word_array.shape[0]-1):
        if idx in indices and idx < len(word_array)-1:
            if word_array[idx+1] in all_tickers:
                new_list.append(''.join(word_array[idx:idx+2]))
                idx += 2
                continue
            else:
                # if the string following '$' is not in all ticker list and not  '_' + 'delimiter' situation
                idx += 2
                continue
        new_list.append(word_array[idx])
        idx += 1

    text = ' '.join([a for a in new_list if a not in ['AT_USER', 'URL', '@', 'rt', 'via']])
    text = replaceMultiExclamationMark(text)
    text = replaceMultiDotMark(text)
    text = replaceMultiStopMark(text)
    text = remove_symbols(text)
    return text

def lag(start_date, gap):
    #start_date is type of pandas.Timestamp
    return start_date - timedelta(gap)
###################################################################################
    #fill in with proper parameters. see original python file for example
import dataframe_preprocessing as preprocessing_step
preprocessing_step.shift_asset_sensitive(df1,'assetName','value','time',3)
preprocessing_step.shift_asset_sensitive(df2,'assetName','value','time',1)
preprocessing_step.mergedf(newsdf, mkt_dataframe, ['time', 'assetName'])
preprocessing_step.elim_rows(merged, "label", "assetName")
###################################################################################
ticker_folders = ['XOM', 'RDS-B', 'PTR', 'CVX', 'TOT', 'BP', 'BHP', 'SNP', 'SLB', 'BBL', 'AAPL', 'PG', 'BUD', 'KO',
                  'PM', 'TM', 'PEP', 'UN', 'UL', 'MO', 'JNJ', 'PFE', 'NVS', 'UNH', 'MRK', 'AMGN', 'MDT', 'ABBV', 'SNY',
                  'CELG', 'AMZN', 'BABA', 'WMT', 'CMCSA', 'HD', 'DIS', 'MCD', 'CHTR', 'UPS', 'PCLN', 'NEE', 'DUK', 'D',
                  'SO', 'NGG', 'AEP', 'PCG', 'EXC', 'SRE', 'PPL', 'IEP', 'HRG', 'CODI', 'REX', 'SPLP', 'PICO', 'AGFS',
                  'BCH', 'BSAC', 'BRK-A', 'JPM', 'WFC', 'BAC', 'V', 'C', 'HSBC', 'MA', 'GE', 'MMM', 'BA', 'HON', 'UTX',
                  'LMT', 'CAT', 'GD', 'DHR', 'ABB', 'GOOG', 'MSFT', 'FB', 'T', 'CHL', 'ORCL', 'TSM', 'VZ', 'INTC',
                  'CSCO']
all_tickers = [i.lower() for i in ticker_folders]

# Add space at the beginning and end to make it properly tokenized 
DELIMITER = ' SEP '
# delete GMRE, cuz there's no tweeted retrived
ticker_folders = ['XOM', 'RDS-B', 'PTR', 'CVX', 'TOT', 'BP', 'BHP', 'SNP', 'SLB', 'BBL', 'AAPL', 'PG', 'BUD', 'KO',
                  'PM', 'TM', 'PEP', 'UN', 'UL', 'MO', 'JNJ', 'PFE', 'NVS', 'UNH', 'MRK', 'AMGN', 'MDT', 'ABBV', 'SNY',
                  'CELG', 'AMZN', 'BABA', 'WMT', 'CMCSA', 'HD', 'DIS', 'MCD', 'CHTR', 'UPS', 'PCLN', 'NEE', 'DUK', 'D',
                  'SO', 'NGG', 'AEP', 'PCG', 'EXC', 'SRE', 'PPL', 'IEP', 'HRG', 'CODI', 'REX', 'SPLP', 'PICO', 'AGFS',
                  'BCH', 'BSAC', 'BRK-A', 'JPM', 'WFC', 'BAC', 'V', 'C', 'HSBC', 'MA', 'GE', 'MMM', 'BA', 'HON', 'UTX',
                  'LMT', 'CAT', 'GD', 'DHR', 'ABB', 'GOOG', 'MSFT', 'FB', 'T', 'CHL', 'ORCL', 'TSM', 'VZ', 'INTC',
                  'CSCO']

dirname = os.path.dirname(os.path.realpath(__file__))

# Basic_Materials = ['XOM','RDS-B','PTR','CVX','TOT','BP','BHP','SNP','SLB','BBL']
output_dir = 'Combined/'
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

all_tickers_df = pd.DataFrame()  # dataframe containing all tickers

price_df = pd.read_csv('stocknet_price.csv')
price_df['Date'] = pd.to_datetime(price_df['Date']).dt.date

for tickerfolder in ticker_folders:

    files = os.listdir(tickerfolder)
    ticker_df = pd.DataFrame()
    for i, file in enumerate(files):
        train = pd.read_json(Path(tickerfolder + '/' + file), lines=True)
        train['text'] = train['text'].apply(self_clean)
        train['text'] = train['text'].apply(tpp.clean)

        text_list = train['text'].tolist()

        day_news = DELIMITER.join(text_list)

        day_df = pd.DataFrame(data={'past0': [day_news]})  # news of that day

        #day_df.index = train['created_at'].dt.date[0]

        day_df['Date'] = train['created_at'].dt.date[0]

        ticker_df = ticker_df.append(day_df)

    #ticker_df.sort_index(inplace=True)
    ticker_df['Name'] = tickerfolder

    ticker_price = price_df.loc[price_df.Name == ticker_df.Name]
    for date in ticker_price['Date']:
        past_3_news = ticker_df.loc[lag(date, 4):lag(date, 1)].values



    merged_df = ticker_price.merge(price_df, how='inner', on=['Date', 'Name'])
    #for i in ticker_df.index:
    #   past1 = ticker_df.iloc[lag(i, 1)]

    '''
    ticker_df['past1'] = ticker_df['past0'].shift(1)
    ticker_df['past2'] = ticker_df['past0'].shift(2)
    ticker_df['past3'] = ticker_df['past0'].shift(3)   
    ticker_df['past4'] = ticker_df['past0'].shift(4)
    ticker_df['past5'] = ticker_df['past0'].shift(5)
    '''
    del ticker_df['past0']
    ticker_df = ticker_df[3:]
    ticker_df.dropna(how='any', inplace=True)
    all_tickers_df = all_tickers_df.append(ticker_df)


price_df = pd.read_csv('stocknet_price.csv')
price_df['Date'] = pd.to_datetime(price_df['Date']).dt.date

merged_df = pd.merge(all_tickers_df, price_df, how='inner', on=['Date', 'Name'])
merged_df = merged_df.sort_values(['Date'])


'''
train= merged_df.sample(frac=0.85,random_state=43)
valid_test_df= merged_df.drop(train.index)
valid = valid_test_df.sample(frac=0.5)
test = valid_test_df.drop(valid.index)

vt_msk = np.random.rand(len(merged_df)) < 0.8
train_df = merged_df[vt_msk]
valid_test_df = merged_df[~vt_msk]

valid_msk = np.random.rand(len(valid_test_df)) < 0.5
valid_df = valid_test_df[valid_msk]
test_df = valid_test_df[~valid_msk]
'''
print(len(merged_df))

train_start_date = datetime.datetime.strptime('2014-01-01', '%Y-%m-%d').date()
dev_start_date = datetime.datetime.strptime('2015-08-01', '%Y-%m-%d').date()
test_start_date = datetime.datetime.strptime('2015-10-01', '%Y-%m-%d').date()

train_df = merged_df[merged_df['Date'] < dev_start_date]
valid_df = merged_df[(dev_start_date < merged_df['Date']) & (merged_df['Date'] < test_start_date)]
test_df = merged_df[test_start_date < merged_df['Date']]

train_df.to_csv(Path(output_dir + 'train.stocknet'), index=None)
valid_df.to_csv(Path(output_dir + 'valid.stocknet'), index=None)
test_df.to_csv(Path(output_dir + 'test.stocknet'), index=None)
