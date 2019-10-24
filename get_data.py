from function import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import urllib.parse
import requests, json, time


def get_history_data(symbol, length, start_time):
    start_time = urllib.parse.quote(str(start_time))
    url = 'https://www.bitmex.com/api/v1/trade/bucketed?binSize=1m&partial=false&symbol={}&count={}&reverse=false&startTime={}'.format(symbol,length,start_time)
    req = requests.get(url)
    req = json.loads(req.content)
    df = pd.DataFrame(columns=['open','high','low','close','volume','trades'])
    try:
        for i in range(len(req)):
            timestamp = req[i]['timestamp']
            date = timestamp[:10]
            time = timestamp[11:19]
            timestamp = date+' '+time
            timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            timestamp += timedelta(hours=8)  # 轉成臺灣時間
            df.at[timestamp,'open'] = req[i]['open']
            df.at[timestamp,'high'] = req[i]['high']
            df.at[timestamp,'low'] = req[i]['low']
            df.at[timestamp,'close'] = req[i]['close']
            df.at[timestamp,'trades'] = req[i]['trades']
            df.at[timestamp,'volume'] = req[i]['volume']
        df.index.name = 'date'
        df.sort_index()
    except:
        pass
    return df

def build_history_dataframe(symbol, start_time):
    file_name = start_time  # 寫檔名用
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    start_time -= timedelta(hours=8)  # 轉成交易所時間
    local_time = datetime.now()
    local_time -= timedelta(hours=8)  # 轉成交易所時間
    # 每次的上限是750，所以每次都抓750分鐘的資料
    print('資料回填中...')
    all_df = pd.DataFrame()
    while start_time < local_time:
        df = get_history_data(symbol, 750, start_time)
        start_time += timedelta(minutes=750)
        all_df = pd.concat([all_df, df], axis=0)
        time.sleep(1)  # 間隔一下，避免被鎖
    all_df.to_csv('1m_data_from_{}.csv'.format(file_name)))


if __name__=='__main__':
    symbol = 'XBTUSD'            
    start_time = '2019-10-22 00:00:00'
    build_history_dataframe(symbol, start_time)


    