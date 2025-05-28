import os

from test import aa

from MarketData import data
#from CTPTrader import g_ctptrader

#执行策略
def OnBar(level):
    print(f"子进程：（{os.getpid()}）,这是straategytest策略，OnBar{level} say Hello!!!")
    a = aa()
    a.say()

    if len(data[level].Open) >0 :
        print(f'###最新K线，开盘价:{data[level].Open[0]},收盘价:{data[level].Close[0]},最高价:{data[level].High[0]},最低价:{data[level].Low[0]}')

    print("开始查询报单")
    #g_ctptrader.query_order()