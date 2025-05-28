from copy import deepcopy
#from CTPTrader import g_ctptrader
from CTPTrader import CTPTrader
import re
from datetime import datetime
import time

from MysqlDB import g_logger


#包含bar和市场快照数据的类
class MarketData:
    def __init__(self):
        self.High = []
        self.H = self.High
        self.Open = []
        self.O = self.Open
        self.Low = []
        self.L = self.Low
        self.Close = []
        self.C = self.Close
        self.OpenInt = []
        self.Vol = []
        self.V = self.Vol
        self.Turnover = []
        self.UpdateTime = []
        self.TradingDay = []
        self.InsideVol = 0 #内盘，主动卖出的量
        self.OutsideVol =0 #外盘，主动买入的量
        self.BarType = 1
        self.BarInterval = 15
        self.BigPointValue = 1
        self.ContractUnit = 10
        self.CurrencyName = '人民币'
        self.CurrencySymbol = '￥'
        self.ExchangeName = '上期所'
        self.ExpiredDate = 202301.000
        self.MarginRatio = 0.16
        self.SymbolName = ''
        self.SymbolType = ''
        self.MinMove = 1.0
        self.PriceScale = 1.0

        self.HasNewOrder = False #是否新下报单

    def set_bar_freq(self,freq):
        self.bar_freq = freq
        if freq < 60:
            self.BarType = 1
            self.BarInterval = freq
        elif freq >= 24*60:
            self.BarType = 0
            self.BarInterval = freq/(24*60)
    def set_instrumentid(self,instrumentid):
        self.instrumentid = instrumentid
        self.Symbol = instrumentid
        nums = re.findall(r'\d+',instrumentid)
        num_strs = ''.join(nums)
        num = int(num_strs)
        num = num + 200000
        return num

    def set_symbolname(self,symbolname):
        self.SymbolName = symbolname

    def set_SymbolType(self,SymbolType):
        self.SymbolType = SymbolType

    def set_minmove(self,minmove):
        self.MinMove = minmove

    def set_pricescale(self,pricescale):
        self.PriceScale = pricescale

    def get_instrumentid(self):
        return self.instrumentid

    def set_margin_ratio(self,ratio):
        self.MarginRatio = ratio

    def set_exchangeid(self,ExchangeID):
        self.ExchangeID = ExchangeID
        if ExchangeID == 'SHFE':
            self.ExchangeName = '上期所'
        elif ExchangeID == 'DCE':
            self.ExchangeName = '大商所'
        elif ExchangeID == 'CZCE':
            self.ExchangeName = '郑商所'
        elif ExchangeID == 'CFFEX':
            self.ExchangeName = '中金所'

    #判断是否是同一个合约
    def is_the_instrument(self,instrumentid):
        if self.instrumentid == instrumentid or self.instrumentid == instrumentid.decode("utf-8"):
            return True
        else:
            return False

    def putbar(self,bar):
        self.High.insert(0,bar["HighPrice"])
        self.Open.insert(0, bar["OpenPrice"])
        self.Low.insert(0, bar["LowPrice"])
        self.Close.insert(0, bar["LastPrice"])
        self.OpenInt.insert(0, bar["OpenInterest"])
        self.Vol.insert(0, bar["BarVolume"])
        self.Turnover.insert(0, bar["BarTurnover"])
        self.UpdateTime.insert(0, bar["UpdateTime"])
        self.TradingDay.insert(0, bar["TradingDay"])
        self.InsideVol += bar["SVolume"]
        self.OutsideVol += bar["BVolume"]

    def puttickdata(self,tickdata):
        self.tickdata = deepcopy(tickdata)

    #获得当前Bar的日信息
    def Day(self):
        return int(self.TradingDay[0].decode("utf-8"))%100

    # 获得当前Bar的小时信息
    def Hour(self):
        time_obj = datetime.strptime(self.UpdateTime[0].decode("utf-8"), "%H:%M:%S")
        return time_obj.hour

    def MilliSecond(self):
        return 0

    # 获得当前Bar的分钟信息
    def Minute(self):
        time_obj = datetime.strptime(self.UpdateTime[0].decode("utf-8"), "%H:%M:%S")
        return time_obj.minute

    # 获得当前Bar的月份信息
    def Month(self):
        time_obj = datetime.strptime(self.UpdateTime[0].decode("utf-8"), "%H:%M:%S")
        return time_obj.month

    # 获得当前Bar的秒信息
    def Second(self):
        time_obj = datetime.strptime(self.UpdateTime[0].decode("utf-8"), "%H:%M:%S")
        return time_obj.second

    # 获得当前Bar的年信息
    def Year(self):
        time_obj = datetime.strptime(self.UpdateTime[0].decode("utf-8"), "%H:%M:%S")
        return time_obj.year

    #当前公式应用商品数据的Bar总数
    def BarCount(self):
        return len(self.High)

    #当前公式应用商品当前Bar的状态值
    def BarStatus(self):
        if len(self.Open) <= 1:
            return 0
        else:
            return 2

    # 当前公式应用商品在当前Bar的索引值
    def CurrentBar(self):
        return 0

    #行情函数
    def Q_AskPrice(self,nIndex):
        if nIndex == 0:
            return self.tickdata['AskPrice1']
        if nIndex == 1:
            return self.tickdata['AskPrice2']
        if nIndex == 2:
            return self.tickdata['AskPrice3']
        if nIndex == 3:
            return self.tickdata['AskPrice4']
        if nIndex == 4:
            return self.tickdata['AskPrice5']

    def Q_AskVol(self,nIndex):
        if nIndex == 0:
            return self.tickdata['AskVolume1']
        if nIndex == 1:
            return self.tickdata['AskVolume2']
        if nIndex == 2:
            return self.tickdata['AskVolume3']
        if nIndex == 3:
            return self.tickdata['AskVolume4']
        if nIndex == 4:
            return self.tickdata['AskVolume5']

    def Q_BidPrice(self,nIndex):
        if nIndex == 0:
            return self.tickdata['BidPrice1']
        if nIndex == 1:
            return self.tickdata['BidPrice2']
        if nIndex == 2:
            return self.tickdata['BidPrice3']
        if nIndex == 3:
            return self.tickdata['BidPrice4']
        if nIndex == 4:
            return self.tickdata['BidPrice5']

    def Q_BidVol(self,nIndex):
        if nIndex == 0:
            return self.tickdata['BidVolume1']
        if nIndex == 1:
            return self.tickdata['BidVolume2']
        if nIndex == 2:
            return self.tickdata['BidVolume3']
        if nIndex == 3:
            return self.tickdata['BidVolume4']
        if nIndex == 4:
            return self.tickdata['BidVolume5']

    def Q_InsideVol(self):
        return self.InsideVol

    def Q_Last(self):
        return self.tickdata['LastPrice']

    def Q_LastTime(self):
        hour = int( self.tickdata['UpdateTime'][0:2])
        min = int(self.tickdata['UpdateTime'][3:5])
        sec = int(self.tickdata['UpdateTime'][6:8])
        result = hour/100+min/10000+sec/1000000
        return result

    def Q_LowerLimit(self):
        return self.tickdata['LowerLimitPrice']

    def Q_OpenInt(self):
        return self.tickdata['OpenInterest']

    def Q_OutsideVol(self):
        return self.OutsideVol

    def Q_PriceChg(self):
        return self.tickdata['LastPrice'] - self.tickdata['PreClosePrice']

    def Q_TurnOver(self):
        return self.tickdata['Turnover']

    #产生一个多头建仓操作
    def Buy(self,share,price):
        self.HasNewOrder = True
        g_logger.info(f'多头建仓操作，合约：{self.instrumentid},建仓数量：{share},价格：{price}')
        #print(f'Buy ExchangeID={self.ExchangeID},instrumentid={self.instrumentid},price={price},share={share}')
        CTPTrader.get_instance().buy_open(self.ExchangeID,self.instrumentid,price,share)

    #产生一个空头平仓操作
    def BuyToCover(self,share,price):
        CTPTrader.get_instance().ShortClose(self.ExchangeID, self.instrumentid,price,share,0)

    #产生一个多头平仓操作
    def Sell(self,share,price):
        CTPTrader.get_instance().LongClose(self.ExchangeID, self.instrumentid, price, share, 0)

    #产生一个空头建仓操作
    def SellShort(self,share,price):
        #print(f'SellShort ExchangeID={self.ExchangeID},instrumentid={self.instrumentid},price={price},share={share}')
        g_logger.info(f'空头建仓操作，合约：{self.instrumentid},建仓数量：{share},价格：{price}')
        CTPTrader.get_instance().sell_open(self.ExchangeID, self.instrumentid, price, share)

    #针对当前公式应用的帐户、商品发送撤单指令
    def A_DeleteOrder(self):
        self.HasNewOrder = True
        g_logger.info(f'对合约：{self.instrumentid},送撤单指令')
        CTPTrader.get_instance().delete_order(self.instrumentid)

    # 回当前公式应用的帐户下当前商品的最后一个未成交委托单索引，按输入参数为条件
    # BuyOrSell:1-买，2-卖
    # EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def A_GetLastOpenOrderIndex(self,BuyOrSell,EntryOrExit,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetLastOpenOrderIndex(self.instrumentid,BuyOrSell,EntryOrExit)

    # 返回指定帐户下当前商品的最后一个当日委托单索引，按输入参数为条件
    # BuyOrSell:1-买，2-卖
    # EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def A_GetLastOrderIndex(self,BuyOrSell,EntryOrExit,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetLastOrderIndex(self.instrumentid, BuyOrSell, EntryOrExit)

    #返回当前公式应用的帐户下当前商品的未成交委托单数量
    def A_GetOpenOrderCount(self,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderCount(self.instrumentid)

    #返回当前公式应用的帐户下当前商品的当日委托单数量
    def A_GetOrderCount(self,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderCount(self.instrumentid)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的买卖类型
    def A_OpenOrderBuyOrSell(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderBuyOrSell(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的开平仓状态
    def A_OpenOrderEntryOrExit(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderEntryOrExit(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的成交数量
    def A_OpenOrderFilledLot(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderFilledLot(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的成交价格
    def A_OpenOrderFilledPrice(self,nIndex,accountIndex):
       ''' if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''
       return CTPTrader.get_instance().GetOpenOrderFilledPrice(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的委托数量
    def A_OpenOrderLot(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderLot(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的委托价格
    def A_OpenOrderPrice(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderPrice(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的状态。
    def A_OpenOrderStatus(self,nIndex,accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOpenOrderStatus(self.instrumentid,nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的委托时间。
    def A_OpenOrderTime(self,nIndex,accountIndex):
        return CTPTrader.get_instance().GetOpenOrderTime(self.instrumentid,nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的买卖类型。
    def A_OrderBuyOrSell(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderBuyOrSell(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的撤单数量。
    def A_OrderCanceledLot(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderCanceledLot(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的开平仓状态。
    def A_OrderEntryOrExit(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderEntryOrExit(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交数量。
    def A_OrderFilledLot(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderFilledLot(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交价格。
    def A_OrderFilledPrice(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderFilledPrice(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托数量。
    def A_OrderLot(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderLot(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托价格。
    def A_OrderPrice(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderPrice(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的状态。
    def A_OrderStatus(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderStatus(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托时间。
    def A_OrderTime(self, nIndex, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetOrderTime(self.instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的买入持仓。
    # type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def A_BuyPosition(self, accountIndex,type=0):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetBuyPosition(self.instrumentid,type)

    # 返回当前公式应用的帐户下当前商品的买入持仓盈亏。
    def A_BuyProfitLoss(self, accountIndex):
       return CTPTrader.get_instance().GetBuyProfitLoss(self.instrumentid)

    # 返回当前公式应用的帐户下当前商品的持仓盈亏。
    def A_PositionProfitLoss(self, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetPositionProfitLoss(self.instrumentid)

    # 返回当前公式应用的帐户下当前商品的卖出持仓。
    # type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def A_SellPosition(self, accountIndex,type=0):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetSellPosition(self.instrumentid,type)

    # 返回当前公式应用的帐户下当前商品的卖出持仓盈亏。
    def A_SellProfitLoss(self, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetSellProfitLoss(self.instrumentid)

    # 针对当前公式应用的帐户、商品发送委托单
    #BuyOrSell:1-buy ,2-sell
    #EntryOrExit:1-开仓,5-平今仓,6-平昨,7-平仓
    def A_SendOrder(self, BuyOrSell,EntryOrExit,Lot, Price, accountIndex):
        self.HasNewOrder = True
        info = 'A_SendOrder:'
        if BuyOrSell == 1:
            if EntryOrExit == 1:
                info += '多头建仓报单'
                CTPTrader.get_instance().buy_open(self.ExchangeID, self.instrumentid, Price, Lot)
            elif EntryOrExit == 5:
                info += '多头平今仓报单'
                CTPTrader.get_instance().LongClose(self.ExchangeID, self.instrumentid, Price, Lot,1)
            elif EntryOrExit == 6:
                info += '多头平昨仓报单'
                CTPTrader.get_instance().LongClose(self.ExchangeID, self.instrumentid, Price, Lot, 2)
            else:
                info += '多头平仓报单'
                CTPTrader.get_instance().LongClose(self.ExchangeID, self.instrumentid, Price, Lot)
        else:
            if EntryOrExit == 1:
                info += '空头建仓报单'
                CTPTrader.get_instance().sell_open(self.ExchangeID, self.instrumentid, Price, Lot)
            elif EntryOrExit == 5:
                info += '空头平今仓报单'
                CTPTrader.get_instance().ShortClose(self.ExchangeID, self.instrumentid, Price, Lot, 1)
            elif EntryOrExit == 6:
                info += '空头平昨仓报单'
                CTPTrader.get_instance().ShortClose(self.ExchangeID, self.instrumentid, Price, Lot, 2)
            else:
                info += '空头平仓报单'
                CTPTrader.get_instance().ShortClose(self.ExchangeID, self.instrumentid, Price, Lot)

            info += f' 合约：{self.instrumentid},平仓数量：{Lot},价格：{Price}'
            g_logger.info(info)

    #返回当前公式应用的帐户下当前商品的当日买入持仓。
    def A_TodayBuyPosition(self, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetTodayBuyPosition(self.instrumentid)

    #返回当前公式应用的帐户下当前商品的当日卖出持仓。
    def A_TodaySellPosition(self, accountIndex):
        '''if self.HasNewOrder == True:
            CTPTrader.get_instance().query_order()
            time.sleep(1)
            CTPTrader.get_instance().query_trade()
            self.HasNewOrder = False
            time.sleep(1)'''

        return CTPTrader.get_instance().GetTodaySellPosition(self.instrumentid)

#定义全局变量MarketData组成的列表
data = []

#策略代码文件
g_settingmap = {'strategy_file':'','tttxy':''}



