import os
import time
from multiprocessing import Process,freeze_support

import pymysql
from dbutils.pooled_db import PooledDB

from CTPTrader import CTPTrader
from MysqlDB import MysqlDB
#from CTPTrader import g_ctptrader

from MarketData import MarketData
from MarketData import data
from MarketData import g_settingmap

DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'passwd': '123456',
    'db': 'traderdb',
    'charset': 'utf8'
}

def run_test():
    print(f'子进程：（{os.getpid()}）开始！')
    while 1:
        print("do")
        time.sleep(5)

def run_a_sub_proc(strategy):
    print(f'子进程：（{os.getpid()}）开始！')

    g_settingmap['strategy_file'] = strategy['filename']
    print(f'子进程：（{os.getpid()}）run_a_sub_proc g_strategy_file={g_settingmap["strategy_file"]}')

    #策略里有几个数据源，data列表就添加几个元素
    bar_freq = 1
    for i in strategy['ds']:
        mark_data = MarketData()
        mark_data.set_instrumentid(i['datasource'])
        mark_data.set_exchangeid(i['ExchangeID'])
        print(f'子进程：（{os.getpid()}）datasource={i["datasource"]}')

        #从freq中分解出数字和字符串
        freq_num = ''.join([k for k in i['freq'] if k.isdigit()])
        freq_str = ''.join([k for k in i['freq'] if k.isalpha()])
        print(f'freq={i["freq"]},num={freq_num},str={freq_str}')
        freq = int(freq_num)
        if freq_str == 'm':
            bar_freq = freq
        elif freq_str == 'h':
            bar_freq = freq*60
        elif freq_str == 'd':
            bar_freq = freq*24*60
        else:
            bar_freq = freq

        mark_data.set_bar_freq(bar_freq)
        data.append(mark_data) #将MarketData作为元素添加到data列表中

    print(f'子进程：（{os.getpid()}）')

    from account_info import my_future_account_info_dict
    future_account = my_future_account_info_dict['SimNow']

    p_ctptrader = CTPTrader.get_instance()
    #p_ctptrader.set_dbpool(db_pool)
    p_ctptrader.Join()
    print(f'子进程：（{os.getpid()}）结束！')

#进程池运行多进程
def run__pool():
    # 创建数据库连接池
    #db_pool = PooledDB(pymysql, 5, **DB_CONFIG)  # 5是连接池的大小

    #print(f'gloabl db_pool={id(db_pool)}')

    #查询策略
    db = MysqlDB()
    #db.set_dbpool(db_pool)
    strategys = db.qry_enable_strategy()
    enable_cnt = len(strategys)#使能的策略个数
    print(strategys)
    if enable_cnt == 0:
        return

    print(f"####strategy len={len(strategys)},enable_cnt={enable_cnt}")
    print(strategys)
    cnt = 0

    processes =[]
    for i in strategys:
        cnt += 1
        print(f'!!!begin run process {cnt}')
        i['ds'] = db.qry_strategy_ds(i['strategyid'])
        p = Process(target=run_a_sub_proc,args=(i,))#每个策略一个进程
        #p = Process(target=run_test)
        processes.append(p)
        p.start()
        #p.join()

    for p in processes:
        p.join()


if __name__ == '__main__':
    freeze_support() #打包后子进程拉不起来，调用这个函数后正常
    run__pool()

    #test
    '''db = MysqlDB("localhost", "root", "123456", "traderdb")
    pOrder = {'BrokerID': b'9999', 'InvestorID': b'210744', 'reserve1': b'cu2310', 'OrderRef': b'1', 'UserID': b'210744', 'OrderPriceType': b'2', 'Direction': b'1', 'CombOffsetFlag': b'0', 'CombHedgeFlag': b'1', 'LimitPrice': 68630.0, 'VolumeTotalOriginal': 1, 'TimeCondition': b'3', 'GTDDate': b'', 'VolumeCondition': b'1', 'MinVolume': 1, 'ContingentCondition': b'1', 'StopPrice': 0.0, 'ForceCloseReason': b'0', 'IsAutoSuspend': 0, 'BusinessUnit': b'9999cac', 'RequestID': 0, 'OrderLocalID': b'         544', 'ExchangeID': b'SHFE', 'ParticipantID': b'9999', 'ClientID': b'9999210722', 'reserve2': b'cu2310', 'TraderID': b'9999cac', 'InstallID': 1, 'OrderSubmitStatus': b'3', 'NotifySequence': 1, 'TradingDay': b'20230921', 'SettlementID': 1, 'OrderSysID': b'        2001', 'OrderSource': b'', 'OrderStatus': b'0', 'OrderType': b'', 'VolumeTraded': 1, 'VolumeTotal': 0, 'InsertDate': b'20230920', 'InsertTime': b'17:53:00', 'ActiveTime': b'', 'SuspendTime': b'', 'UpdateTime': b'', 'CancelTime': b'', 'ActiveTraderID': b'9999cac', 'ClearingPartID': b'', 'SequenceNo': 1063, 'FrontID': 1, 'SessionID': 1750225850, 'UserProductInfo': b'', 'StatusMsg': b'\xc8\xab\xb2\xbf\xb3\xc9\xbd\xbb', 'UserForceClose': 0, 'ActiveUserID': b'', 'BrokerOrderSeq': 2388, 'RelativeOrderSysID': b'', 'ZCETotalTradedVolume': 0, 'IsSwapOrder': 0, 'BranchID': b'', 'InvestUnitID': b'', 'AccountID': b'', 'CurrencyID': b'', 'reserve3': b'', 'MacAddress': b'', 'InstrumentID': b'cu2310', 'ExchangeInstID': b'cu2310', 'IPAddress': b''}
    order_status = db.order_status(pOrder)
    print(order_status)'''







