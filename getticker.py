import requests
import json
from retrying import retry
import pandas as pd
import sqlite3
import schedule
import time
from datetime import datetime
from itertools import product
import pickle
from colorama import Fore, Back, Style
#import sys

ticker_cols = ['best_bid', 'best_ask', 'total_bid_depth', 'total_ask_depth', 'ltp', 'volume', 'volume_by_product']
real_fields = ['total_bid_depth', 'total_ask_depth', 'volume', 'volume_by_product']
timeformat = '%Y-%m-%d %H:%M:%S.%f'
create_table = "create table ticker(exec_time varchar(64), best_bid int, best_ask int, total_bid_depth real, total_ask_depth real, ltp int, volume real, volume_by_product real);"


def ntime():
    return datetime.now().strftime(timeformat)

def create_ticker_table():
    s = [', '+s+' int' for s in ticker_cols]
    s = ''.join(s)
    conn = sqlite3.connect('ticker.db')
    c = conn.cursor()
    c.execute(create_table)
    conn.commit()
    print('ticker table created')
    return create_table

class bitFlyer():
    def __init__(self):
        try:
            with open('bitFlyer.binaryfile', 'rb') as f:
                data = pickle.load(f)
                self.tickers = data.tickers.copy()
                self.boards = data.boards.copy()
        except:
            self.tickers = {}
            self.boards = {}
            
    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def get_api(self, dtype):
        endpoint = "https://api.bitflyer.jp/v1/"
        print(endpoint + dtype)
        response = requests.get(endpoint + dtype)        
        
        #datasize = sys.getsizeof(response)
        #print('response data size: ', datasize, 'bite')
        #print('estimated communication volume: ', datasize/60, 'bps')
        
        print(response)
        response = response.text
        return json.loads(response)
    
    def set_new_board(self):
        key = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print('requesting board ...')
        try:
            self.boards[key] = self.get_api('board')
            print('')
        except:
            print('request board failed 5times.')
            print('')
            return ''
        
    def save(self):
        with open('bitFlyer.binaryfile', 'wb') as f:
            pickle.dump(self , f)

@retry(stop_max_attempt_number=5, wait_fixed=500)
def get_api(dtype):
    endpoint = "https://api.bitflyer.jp/v1/"
    print(endpoint + dtype)
    response = requests.get(endpoint + dtype)
    print(response)
    
    #datasize = sys.getsizeof(response)
    #print('response data size: ', datasize, 'bite')
    #print('estimated communication volume: ', datasize/15, 'bps')
    
    response = response.text
    return json.loads(response)

@retry(stop_max_attempt_number=5, wait_fixed=1000)
def get_data():
    start_time = ntime()
    print('-------------------------------------')
    print('job started at: ', start_time)
    print('')
    print('requesting ticker ...')
    try:
        ticker = get_api("ticker")
        print('')
    except:
        print('request ticker failed 5times.')
        print('')
        return ''
    print('< info >')
    print('ticker timestamp: ', ticker['timestamp'])
    print(Fore.GREEN+'best_ask: ', ticker['best_ask'])
    print(Fore.RED+'best_bid: ', ticker['best_bid'])
    print(Style.RESET_ALL+'latest transaction price: ', ticker['ltp'])
    print('')
    print('processing ...')
    
    for k in ['best_bid', 'best_ask', 'ltp']:
        ticker[k] = int(ticker[k])
    del ticker['product_code']
    del ticker['timestamp']
    del ticker['tick_id']
    del ticker['best_bid_size']
    del ticker['best_ask_size']
    del ticker['state']  # 20200926 追加
    del ticker['market_bid_size']  # 20200926 追加
    del ticker['market_ask_size']  # 20200926 追加
    
    print(ticker)  # ticker の中身が変わった可能性
    v = tuple([start_time]+list(ticker.values()))
    
    #print(v)   # v が不正の可能性高
    ticker_sql = "insert into ticker values ('%s' ,'%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s');" % v  #ここでエラー
    
    print(Fore.GREEN+'processed')
    print(Style.RESET_ALL+'')
    print('connecting ticker.db ...')
    conn = sqlite3.connect('ticker.db')
    c = conn.cursor()
    print('connected') 
    print('ticker executing ...')
    c.execute(ticker_sql)
    conn.commit()
    print(Fore.GREEN+'executed')
    finish_time = ntime()
    delta = datetime.strptime(finish_time, timeformat) - datetime.strptime(start_time, timeformat)
    print('')
    print(Fore.GREEN+'job execution time: ', delta)
    print('job finished at: ', ntime())
    print(Style.RESET_ALL+'')

def job():
    for i in range(0,4):
        try:
            get_data()
        except:
            print(Fore.RED+'job failed 5times.', Style.RESET_ALL+'')
            return ''
        time.sleep(14)

def set_schedule():
    h = [str(i).zfill(2) for i in range(24)]
    m = [str(i).zfill(2) for i in range(60)]
    hm = product(h,m)
    times = list(map(lambda x: x[0]+':'+x[1],hm))
    for t in times:
        schedule.every().day.at(t).do(job).tag(t)
    return times

if __name__ == '__main__':
    #schedule.clear()
    times = set_schedule()
    print(Style.RESET_ALL+'')
    print('jobs set: ', len(times))
    print('loop start at: ', ntime())
    print('')
    while True:
        schedule.run_pending()
        time.sleep(1)
