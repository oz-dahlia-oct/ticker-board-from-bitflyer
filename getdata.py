import requests
import json
from retrying import retry
import pandas as pd
import sqlite3
import schedule
import time
from datetime import datetime
from itertools import product
#import sys

@retry(stop_max_attempt_number=5, wait_fixed=1000)
def get_api(path):
    endpoint = "https://api.bitflyer.jp/v1/"
    print(endpoint + path)
    response = requests.get(endpoint + path)
    print(response)
    
    #datasize = sys.getsizeof(response)
    #print('response data size: ', datasize, 'bite')
    #print('estimated communication volume: ', datasize/60, 'bps')
    
    response = response.text
    return json.loads(response)

timeformat = '%Y-%m-%d %H:%M:%S.%f'
def ntime():
    return datetime.now().strftime(timeformat)

def set_data(dataframe, df_type, mid_price):
    df = dataframe
    df['amount'] = df['price']*df['size']
    df['f_price'] = 1000*df['price']/mid_price-999
    if df_type == 'bids':
        df['f_price'] = df['f_price']*(-1)
    df = df.loc[df['f_price']<=250, :]
    df.sort_values('f_price', inplace=True)
    sum_amount = 0
    for i, item in df.iterrows():
        sum_amount += item['amount']
        df.loc[i, 'sum_amount'] = sum_amount
    df['f_price'] = df['f_price'].astype(int)
    df = df.groupby('f_price', as_index=False).first()
    for i in range(1,250):
        l = len(df.loc[df['f_price']==i, :])
        if l==0:
            d = df.loc[df['f_price']==i-1, 'sum_amount']
            try:
                intd = int(d)
            except:
                intd = 0
            append = pd.DataFrame([{'f_price': i ,'price': 0,'size': 0,'amount': 0,'sum_amount': intd}])
            df = pd.concat([df,append])
    if df_type == 'bids':
        df['f_price'] = df['f_price']*(-1)
    return df

def set_data2(dataframe, df_type, mid_price):
    df = dataframe
    df['amount'] = df['price']*df['size']
    df['f_price'] = df['price']-mid_price
    if df_type == 'bids':
        df['f_price'] = -df['f_price']
    df['f_price'] = 1000*df['f_price']/mid_price+1
    df = df.loc[df['f_price']<=250, :]
    cols = list(df.columns)
    k = product([0],[0],[0],range(1,250))
    cols = list(df.columns)
    ap = pd.DataFrame(k, columns=cols)
    df = pd.concat([df, ap])
    df['f_price'] = df['f_price'].astype(int)
    agg = df.groupby('f_price', as_index=False).sum()[['f_price', 'amount']]
    sum_amount = 0
    agg['sum_amount'] = 0
    agg.sort_values('f_price', inplace=True)
    for i, item in agg.iterrows():
        sum_amount += item['amount']
        agg.loc[agg['f_price']==item['f_price'], 'sum_amount'] = sum_amount
    if df_type == 'bids':
        agg['f_price'] = -agg['f_price']
    return agg

@retry(stop_max_attempt_number=5, wait_fixed=1000)
def get_data():
    start_time = ntime()
    print('-------------------------------------')
    print('job started at: ', start_time)
    print('')
    print('requesting board ...')
    try:
        board = get_api(path = "board")
    except:
        print('request board failed 5times.')
        print('')
        return ''
    print('')
    #print('requesting ticker ...')
    #try:
        #ticker = get_api(path = "ticker")
    #except:
        #print('request ticker failed 5times.')
        #print('')
        #return ''
    #print('ticker timestamp: ', ticker['timestamp'])
    #v = tuple([start_time]+list(ticker.values()))
    #ticker_sql = "insert into ticker values ('%s' ,'%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s', '%s');" % v
    bids = pd.DataFrame(board['bids'])
    raw_bids = bids.copy()
    asks = pd.DataFrame(board['asks'])
    raw_asks = asks.copy()
    asks_min = asks['price'].min()
    bids_max = bids['price'].max()
    mean_price = (asks_min+bids_max)/2
    print('')
    print('<price>')
    print('min_asks: ', asks_min)
    print('max_bids: ', bids_max)
    print('mean_price: ', mean_price)
    print('mid_price: ', board['mid_price'])
    print('')
    print('processing data ...')
    bids = set_data2(bids, 'bids', mean_price)
    asks = set_data2(asks, 'asks', mean_price)
    data = pd.concat([bids, asks])
    data.sort_values('f_price', inplace=True)
    vals = ["'"+start_time+"'", asks_min, bids_max]+list(data['sum_amount'])
    l = ', '.join([str(k) for k in vals])
    data_len = len(list(data['sum_amount']))
    print('')
    print('data length: ', data_len)
    if data_len!=498:
        raw_asks.to_csv('failed/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S_rawasks.csv'))
        raw_bids.to_csv('failed/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S_rawbids.csv'))
        asks.to_csv('failed/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S_asks.csv'))
        bids.to_csv('failed/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S_bids.csv'))
        data.to_csv('failed/'+datetime.now().strftime('%Y-%m-%d_%H-%M-%S_data.csv'))
        print('invalid data length')
        print('log file saved')
        print('')
        #raise ValueError('')    おかしな板の状態がどれくらい続くかわからないため、とりあえずは例外を吐かずに歯抜けにする
    print('processed')

    print('')
    print('connecting bitflyer.db ...')
    conn = sqlite3.connect('bitflyer.db')
    c = conn.cursor()
    print('connected')

    if data_len==498:
        board_sql = "insert into board values ({});".format(l)
        print('board executing ...')
        c.execute(board_sql)
        conn.commit()
        print('executed')

    #print('ticker executing ...')
    #c.execute(ticker_sql)
    #conn.commit()
    #print('executed')
    finish_time = ntime()
    delta = datetime.strptime(finish_time, timeformat) - datetime.strptime(start_time, timeformat)
    print('')
    print('job execution time: ', delta)
    print('job finished at: ', ntime())
    print('')

def get_data2():
    try:
        get_data()
    except:
        print('job failed 5times.')
        return ''

def create_ticker_table():
    sql = '''create table ticker (
    exec_time varchar(64),
    product_code varchar(64),
    timestamp varchar(64),
    tick_id int,
    best_bid real,
    best_ask real,
    best_bid_size real,
    best_ask_size real,
    total_bid_depth real,
    total_ask_depth real,
    ltp real,
    volume real,
    volume_by_product real
    )
    '''
    print(sql)
    return {'create ticker table':sql}

def create_board_table():
    n = [',n'+str(i).zfill(3)+' real' for i in range(1,250)]
    n.reverse()
    p =  [',p'+str(i).zfill(3)+' real' for i in range(1,250)]
    cols = n+p
    s=''.join(cols)
    sql = "create table board(exec_time varchar(64), asks_min real, bids_max real{})".format(s)
    print(sql)
    conn = sqlite3.connect('bitflyer.db')
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    return {'create board table':sql}

def set_schedule():
    h = [str(i).zfill(2) for i in range(24)]
    m = [str(i).zfill(2) for i in range(60)]
    hm = product(h,m)
    times = list(map(lambda x: x[0]+':'+x[1],hm))
    for t in times:
        schedule.every().day.at(t).do(get_data2).tag(t)
    return times

if __name__ == '__main__':
    #schedule.clear()
    times = set_schedule()
    print('')
    print('jobs set: ', len(times))
    print('loop start at: ', ntime())
    print('')
    while True:
        schedule.run_pending()
        time.sleep(1)
