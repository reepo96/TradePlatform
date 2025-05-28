# encoding:utf-8
import os

from dbutils.pooled_db import PooledDB

from AlgoPlus.CTP.ApiConst import HedgeFlag_Speculation
from AlgoPlus.CTP.ApiStruct import OrderActionField, QryInstrumentField
from AlgoPlus.CTP.TraderApiBase import TraderApiBase
#from AlgoPlus.CTP.AuthenticateHelper import AuthenticateHelper
from AlgoPlus.CTP.ApiStruct import QryInstrumentMarginRateField
import time

from Logger import Logger
from MysqlDB import MysqlDB
from BarSub import BarSub
from MysqlDB import g_logger

'''def singleton(cls):
    _instance = {}

    def _singleton(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return _singleton'''

#@singleton
class CTPTrader(TraderApiBase):
    _instance = None

    def __init__(self, broker_id, td_server, investor_id, password, app_id, auth_code, md_queue=None, flow_path='',
                 private_resume_type=2, public_resume_type=2):
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.my_db = MysqlDB()
        self.has_new_trade = False #是否有新的成交
        self.last_trade_time = 0 #最后一次成交时间
        self.trade_cnt = 0 #连续成交次数
        self.AllInstrumentList = []
        self.AllProductList = []

        print(f'CTPTrader:investor_id={investor_id},password={password},broker_id={broker_id},app_id={app_id},auth_code={auth_code}')

    '''def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CTPTrader,cls).__new__(cls, *args, **kwargs)
        return cls._instance'''

    #@staticmethod
    @classmethod
    def get_instance(cls):
        #实现单一实例
        if cls._instance == None:
            from account_info import my_future_account_info_dict
            future_account = my_future_account_info_dict['SimNow']
            cls._instance = CTPTrader(broker_id=future_account.broker_id,
                                    #td_server=future_account.reserve_server_dict['qita1']['TDServer'],
                                    td_server=future_account.server_dict['TDServer'],
                                    investor_id=future_account.investor_id,
                                    password=future_account.password,
                                    app_id=future_account.app_id,
                                    auth_code=future_account.auth_code,
                                    md_queue=None,
                                    flow_path=future_account.td_page_dir)
        #print(f'CTPTrader::get_instance,return _instance={id(cls._instance)}')
        return cls._instance

    def init_extra(self):
        self.mystatus = 0
        self.is_first_reqaccount = True
        self.last_qryorder_time = 0

    def set_dbpool(self,dbpool:PooledDB):
        self.my_db.set_dbpool(dbpool)

    def qry_account(self):
        print('enter qry_account')
        self.query_trading_account()
        time.sleep(3)

    #多头平仓（AlgoPlus的buy_close,sell_close似乎有问题，不能平今仓）
    #today_flag,是否是今日：0-不确定；1-今日；2-昨日
    def LongClose(self,exchange_id,instrument_id,order_price,order_vol,today_flag=0):
        offset_flag = b'1'
        sell_num = order_vol

        if today_flag == 1:
            offset_flag = b'3'
        elif today_flag == 2:
            offset_flag = b'4'

        if order_vol == 0: #全部平仓
            if exchange_id == "SHFE" or exchange_id == "INE":#这两交易所区分平今平昨
                sell_num = self.GetBuyPosition(instrument_id, 2) #昨仓
                offset_flag = b'4' #平昨
            else:
                sell_num = self.GetBuyPosition(instrument_id, 0)  # 全部仓位
                offset_flag = b'1' #平仓

            self.insert_order(exchange_id=exchange_id,
                              instrument_id=instrument_id,
                              order_price=order_price,
                              order_vol=sell_num,
                              direction=b'1',#多头平仓，买卖方向：卖
                              offset_flag=offset_flag)

            g_logger.info(f'多头平仓操作，合约：{instrument_id},传入平仓数量：0,报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')

            if exchange_id == "SHFE" or exchange_id == "INE":  # 这两交易所区分平今平昨
                sell_num = self.GetBuyPosition(instrument_id, 1)  # 今仓
                offset_flag = b'3'  # 平今
                self.insert_order(exchange_id=exchange_id,
                                  instrument_id=instrument_id,
                                  order_price=order_price,
                                  order_vol=sell_num,
                                  direction=b'1',  # 多头平仓，买卖方向：卖
                                  offset_flag=offset_flag)
                g_logger.info(f'多头平仓操作，合约：{instrument_id},传入平仓数量：0,报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')
        else:#指定数量平仓
            if today_flag==0 and (exchange_id == "SHFE" or exchange_id == "INE"):
                #先平昨仓
                sell_num = self.GetBuyPosition(instrument_id, 2)  # 昨仓
                offset_flag = b'4'  # 平昨
                if sell_num > order_vol:
                    sell_num = order_vol

                if sell_num > 0:
                    self.insert_order(exchange_id=exchange_id,
                                      instrument_id=instrument_id,
                                      order_price=order_price,
                                      order_vol=sell_num,
                                      direction=b'1',  # 多头平仓，买卖方向：卖
                                      offset_flag=offset_flag)
                    g_logger.info(f'多头平仓(平昨仓)，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price}')

                if sell_num < order_vol:
                    offset_flag = b'3'  # 平今
                    sell_num = (order_vol-sell_num)
                    self.insert_order(exchange_id=exchange_id,
                                      instrument_id=instrument_id,
                                      order_price=order_price,
                                      order_vol=sell_num,
                                      direction=b'1',  # 多头平仓，买卖方向：卖
                                      offset_flag=offset_flag)
                    g_logger.info(f'多头平仓(平今)，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price}')
            else: #指定了平今或平昨，或者交易所不是SHFE和INE
                self.insert_order(exchange_id=exchange_id,
                                  instrument_id=instrument_id,
                                  order_price=order_price,
                                  order_vol=sell_num,
                                  direction=b'1',  # 多头平仓，买卖方向：卖
                                  offset_flag=offset_flag)
                g_logger.info(f'多头平仓操作，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')

        # 空头平仓（AlgoPlus的buy_close,sell_close似乎有问题，不能平今仓）
        # today_flag,是否是今日：0-不确定；1-今日；2-昨日
        def ShortClose(self, exchange_id, instrument_id, order_price, order_vol, today_flag=0):
            offset_flag = b'1'
            sell_num = order_vol

            if today_flag == 1:
                offset_flag = b'3'
            elif today_flag == 2:
                offset_flag = b'4'

            if order_vol == 0:  # 全部平仓
                if exchange_id == "SHFE" or exchange_id == "INE":  # 这两交易所区分平今平昨
                    sell_num = self.GetSellPosition(instrument_id, 2)  # 昨仓
                    offset_flag = b'4'  # 平昨
                else:
                    sell_num = self.GetSellPosition(instrument_id, 0)  # 全部仓位
                    offset_flag = b'1'  # 平仓

                self.insert_order(exchange_id=exchange_id,
                                  instrument_id=instrument_id,
                                  order_price=order_price,
                                  order_vol=sell_num,
                                  direction=b'0',  # 空头平仓，买卖方向：买
                                  offset_flag=offset_flag)

                g_logger.info(f'空头平仓操作，合约：{instrument_id},传入平仓数量：0,报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')

                if exchange_id == "SHFE" or exchange_id == "INE":  # 这两交易所区分平今平昨
                    sell_num = self.GetSellPosition(instrument_id, 1)  # 今仓
                    offset_flag = b'3'  # 平今
                    self.insert_order(exchange_id=exchange_id,
                                      instrument_id=instrument_id,
                                      order_price=order_price,
                                      order_vol=sell_num,
                                      direction=b'0',  # 空头平仓，买卖方向：买
                                      offset_flag=offset_flag)
                    g_logger.info(f'空头平仓操作，合约：{instrument_id},传入平仓数量：0,报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')
            else:  # 指定数量平仓
                if today_flag == 0 and (exchange_id == "SHFE" or exchange_id == "INE"):
                    # 先平昨仓
                    sell_num = self.GetSellPosition(instrument_id, 2)  # 昨仓
                    offset_flag = b'4'  # 平昨
                    if sell_num > order_vol:
                        sell_num = order_vol

                    if sell_num > 0:
                        self.insert_order(exchange_id=exchange_id,
                                          instrument_id=instrument_id,
                                          order_price=order_price,
                                          order_vol=sell_num,
                                          direction=b'0',  # 空头平仓，买卖方向：买
                                          offset_flag=offset_flag)
                        g_logger.info(f'空头平仓(平昨仓)，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price}')

                    if sell_num < order_vol:
                        offset_flag = b'3'  # 平今
                        sell_num = (order_vol - sell_num)
                        self.insert_order(exchange_id=exchange_id,
                                          instrument_id=instrument_id,
                                          order_price=order_price,
                                          order_vol=sell_num,
                                          direction=b'0',  # 空头平仓，买卖方向：买
                                          offset_flag=offset_flag)
                        g_logger.info(f'空头平仓(平今)，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price}')
                else:  # 指定了平今或平昨，或者交易所不是SHFE和INE
                    self.insert_order(exchange_id=exchange_id,
                                      instrument_id=instrument_id,
                                      order_price=order_price,
                                      order_vol=sell_num,
                                      direction=b'0',  # 空头平仓，买卖方向：买
                                      offset_flag=offset_flag)
                    g_logger.info(f'空头平仓操作，合约：{instrument_id},传入平仓数量：{order_vol},报单平仓数量：{sell_num},价格：{order_price},offset_flag:{offset_flag}')

    #查询合约保证金率
    def qry_marginrate(self, instrumentid):
        field = QryInstrumentMarginRateField(
            BrokerID= self.broker_id,
            InvestorID= self.investor_id,
            ExchangeID = b'',
            InstrumentID = instrumentid.encode('utf-8'),
            HedgeFlag = HedgeFlag_Speculation
        )
        self.ReqQryInstrumentMarginRate(field, 0)

    # 查询合约保证金率的回应
    def OnRspQryInstrumentMarginRate(self,pMarginRateField,pRspInfo, requestid, bislast):
        print('$$$$$$$$$$$$ enter OnRspQryInstrumentMarginRate')
        print(pMarginRateField)
        from MarketData import data
        for d in data:
            if d.is_the_instrument(pMarginRateField["InstrumentID"]):
                d.set_margin_ratio(pMarginRateField["LongMarginRatioByMoney"])

    # 查询合约
    def qry_instrument(self, instrumentid):
        print(f'enter qry_instrument,instrumentid={instrumentid}')
        field = QryInstrumentField(
            ExchangeID=b'',
            InstrumentID=instrumentid.encode('utf-8')
        )

        self.ReqQryInstrument(field, 0)

    # 查询合约回应
    def OnRspQryInstrument(self, pInstrumentField, pRspInfo, nRequestID, bIsLast):
        from MarketData import data
        #print(f'###OnRspQryInstrument,InstrumentID={pInstrumentField["InstrumentID"]}')
        #print(f'OnRspQryInstrument={pInstrumentField}')

        #只保留期货合约
        if pInstrumentField["ProductClass"] == b'1':
            self.AllInstrumentList.append(pInstrumentField["InstrumentID"].decode('utf-8'))
            productid = pInstrumentField["ProductID"].decode('utf-8')
            if productid not in self.AllProductList:
                self.AllProductList.append(productid)

        if bIsLast == True:
            print(f'OnRspQryInstrument get last info,list={self.AllInstrumentList}')
            with open('allinstrument.txt','w') as subfile:
                #subfile.write(str(self.AllInstrumentList)) #python代码用
                separator = ':'
                str = separator.join(self.AllInstrumentList)
                subfile.write(str)
                '''str = ''
                cnt = 0
                totcnt =0
                for instrument in self.AllInstrumentList:
                    str = str + instrument + ':'
                    cnt +=1
                    totcnt += 1
                    if cnt > 20:
                        str = str + '\r\n'
                        subfile.write(str)
                        str = ''
                        cnt =0'''
                subfile.close()

            with open('allproduct.txt','w') as productfile:
                #productfile.write(str(self.AllProductList))
                separator = ':'
                str = separator.join(self.AllProductList)
                productfile.write(str)
                productfile.close()

        for d in data:
            if d.is_the_instrument(pInstrumentField["InstrumentID"]):
                d.set_symbolname(pInstrumentField["InstrumentName"])
                d.set_SymbolType(pInstrumentField["ProductID"])
                d.set_minmove(pInstrumentField["VolumeMultiple"])

                pricescale = pInstrumentField["PriceTick"]/pInstrumentField["VolumeMultiple"]
                d.set_pricescale(pricescale)

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        #查询资金账户回应函数
        print(f"Enter OnRspQryTradingAccount,子进程：（{os.getpid()}）,父进程={os.getppid()}")
        print(pTradingAccount)

        future_account.set_trading_account(pTradingAccount)

        if self.is_first_reqaccount == True:
            self.is_first_reqaccount = False
            if self.my_db.is_exist_account() == False:
                self.my_db.insert_tradaccount(pTradingAccount)
            else:
                self.my_db.update_tradaccount(pTradingAccount)
        else:
            self.my_db.update_tradaccount(pTradingAccount)

    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, requestid, bislast):
        print(f'enter OnRspAuthenticate,子进程：（{os.getpid()}）,父进程={os.getppid()}')
        #print(pRspAuthenticateField)
        super().OnRspAuthenticate(pRspAuthenticateField, pRspInfo, requestid, bislast)

    def OnRspUserLogin(self, pLogField,pRspInfo, requestid, bislast):
        print(f"enter OnRspUserLogin!!!!!!!,子进程：（{os.getpid()}）,父进程={os.getppid()}")
        print(pLogField['BrokerID'])
        self.mystatus = 1
        super().OnRspUserLogin(pLogField,pRspInfo, requestid, bislast)

    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        #if bIsLast:
        print(f"Enter OnRspQryOrder bIsLast={bIsLast}")
        print(pOrder)
        order_status = self.my_db.order_status(pOrder)
        if order_status == 0:
            self.my_db.insert_orderinfo(pOrder)
        else:
            self.my_db.update_orderinfo(pOrder)

        if bIsLast == True:
            self.last_qryorder_time = int(time.time())
            time.sleep(0.1)
            self.query_trade()

    #自动报过来的订单信息
    def OnRtnOrder(self, pOrder):
        print(f"Enter OnRtnOrder,子进程：（{os.getpid()}）,父进程={os.getppid()}")
        #print(pOrder)
        order_status = self.my_db.order_status(pOrder)
        if order_status == 0:
            self.my_db.insert_orderinfo(pOrder)
        else:
            self.my_db.update_orderinfo(pOrder)

    #错误报单
    def OnErrRtnOrderInsert(self,pOrder, pRspInfo):
        print("Enter OnErrRtnOrderInsert ")
        info = (f'错误报单信息：合约[{pOrder["InstrumentID"]}],OrderRef[{pOrder["OrderRef"]}],数量[{pOrder["VolumeTotalOriginal"]}]'
                f'，价格[{pOrder["LimitPrice"]}]')
        if pOrder["Direction"] == b'0':
            info += '，买卖方向[买]'
        else:
            info += '，买卖方向[卖]'

        if pOrder["CombOffsetFlag"] == b'0':
            info += '，[开仓]'
        elif pOrder["CombOffsetFlag"] == b'1':
            info += '，[平仓]'
        elif pOrder["CombOffsetFlag"] == b'2':
            info += '，[强平]'
        elif pOrder["CombOffsetFlag"] == b'3':
            info += '，[平今]'
        elif pOrder["CombOffsetFlag"] == b'4':
            info += '，[平昨]'
        elif pOrder["CombOffsetFlag"] == b'5':
            info += '，[强减]'
        else:
            info += '，[本地强平]'

        info += pRspInfo["ErrorMsg"]
        g_logger.error(info)


    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        if len(dict(pTrade)) == 0:
            return

        print(f"Enter OnRspQryTrade bIsLast={bIsLast}")
        #print(pTrade)

        if bIsLast == True:
            time.sleep(0.1)
            self.query_position()

        trade_dict = {}
        trade_dict["ExchangeID"] = pTrade["ExchangeID"]
        trade_dict["TradeID"] = pTrade["TradeID"]
        trade_dict["OrderSysID"] = pTrade["OrderSysID"]
        trade_dict["Direction"] = pTrade["Direction"]

        #数据库中不存在才插入
        if self.my_db.is_exist_tradeinf(trade_dict) == False:
            self.my_db.insert_tradeinfo(pTrade)

    # 自动报过来的成交信息
    def OnRtnTrade(self, pTrade):
        print(f"#### Enter OnRtnTrade,子进程：（{os.getpid()}）,父进程={os.getppid()} ")
        #print(pTrade)
        trade_dict = {}
        trade_dict["ExchangeID"] = pTrade["ExchangeID"]
        trade_dict["TradeID"] = pTrade["TradeID"]
        trade_dict["OrderSysID"] = pTrade["OrderSysID"]
        trade_dict["Direction"] = pTrade["Direction"]

        # 数据库中不存在才插入
        if self.my_db.is_exist_tradeinf(trade_dict) == False:
            self.my_db.insert_tradeinfo(pTrade)

        self.has_new_trade = True
        self.last_trade_time = int(time.time())
        self.trade_cnt += 1

        if self.trade_cnt >= 30:
            self.AfterTrade()

    #成交之后的处理
    def AfterTrade(self):
        self.has_new_trade = False
        self.trade_cnt = 0

        self.my_db.clear_investor_position()

        time.sleep(0.1)
        self.query_position()
        time.sleep(0.1)
        self.qry_account()

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        print(f"Enter OnRspQryInvestorPosition bIsLast={bIsLast},子进程：（{os.getpid()}）,父进程={os.getppid()}")
        print(pInvestorPosition)

        if len(dict(pInvestorPosition)) == 0 :
            return

        if self.my_db.is_exist_investor_position(pInvestorPosition) == True:
            self.my_db.update_investor_position(pInvestorPosition)
        else:
            self.my_db.insert_investor_position(pInvestorPosition)

        if bIsLast == True:
            time.sleep(0.1)
            self.qry_account()

    #撤单
    def delete_one_order(self,orderid):
        print("enter delete_one_order")
        order = self.my_db.qry_order_byid(orderid)
        '''action_field = OrderActionField()
        #action_field'''

        print(f'order={order}')

        if order != None:
            #self.ReqOrderAction(action_field)
            self.cancel_order(order["ExchangeID"].encode("utf-8"),order["InstrumentID"].encode("utf-8"),
                              order["OrderRef"].encode("utf-8"),order["OrderSysID"].encode("utf-8"))

    #撤商品所有报单
    def delete_order(self,instrumentid):
        orders = self.my_db.qry_uncomplete_order(instrumentid)
        for i in orders:
            self.cancel_order(i["ExchangeID"].encode("utf-8"), i["InstrumentID"].encode("utf-8"),
                              i["OrderRef"].encode("utf-8"), i["OrderSysID"].encode("utf-8"))

    # 回当前公式应用的帐户下当前商品的最后一个未成交委托单索引，按输入参数为条件
    # BuyOrSell:1-买，2-卖
    # EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def GetLastOpenOrderIndex(self, instrumentid, BuyOrSell, EntryOrExit):
        return self.my_db.GetLastOpenOrderIndex(instrumentid,BuyOrSell, EntryOrExit)

    # 返回指定帐户下当前商品的最后一个当日委托单索引，按输入参数为条件
    # BuyOrSell:1-买，2-卖
     # EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def GetLastOrderIndex(self, instrumentid, BuyOrSell, EntryOrExit):
        return self.my_db.GetLastOrderIndex(instrumentid, BuyOrSell, EntryOrExit)

    # 返回当前公式应用的帐户下当前商品的未成交委托单数量
    def GetOpenOrderCount(self, instrumentid):
        return self.my_db.GetOpenOrderCount(instrumentid)

    # 返回当前公式应用的帐户下当前商品的当日委托单数量
    def GetOrderCount(self, instrumentid):
        return self.my_db.GetOrderCount(instrumentid)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的买卖类型
    def GetOpenOrderBuyOrSell(self, instrumentid,nIndex):
        return self.my_db.GetOpenOrderBuyOrSell(instrumentid,nIndex)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的开平仓状态
    def GetOpenOrderEntryOrExit(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderEntryOrExit(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的成交数量。
    def GetOpenOrderFilledLot(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderFilledLot(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的成交价格。
    def GetOpenOrderFilledPrice(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderFilledPrice(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的委托数量
    def GetOpenOrderLot(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderLot(instrumentid, nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的委托价格
    def GetOpenOrderPrice(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderPrice(instrumentid, nIndex)

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的状态
    def GetOpenOrderStatus(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderStatus(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的委托时间
    def GetOpenOrderTime(self, instrumentid, nIndex):
        return self.my_db.GetOpenOrderTime(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的买卖类型
    def GetOrderBuyOrSell(self, instrumentid, nIndex):
        return self.my_db.GetOrderBuyOrSell(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的撤单数量
    def GetOrderCanceledLot(self, instrumentid, nIndex):
        return self.my_db.GetOrderCanceledLot(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的开平仓状态
    def GetOrderEntryOrExit(self, instrumentid, nIndex):
        return self.my_db.GetOrderEntryOrExit(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交数量
    def GetOrderFilledLot(self, instrumentid, nIndex):
        return self.my_db.GetOrderFilledLot(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交价格
    def GetOrderFilledPrice(self, instrumentid, nIndex):
        return self.my_db.GetOrderFilledPrice(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托数量
    def GetOrderLot(self, instrumentid, nIndex):
        return self.my_db.GetOrderLot(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托价格
    def GetOrderPrice(self, instrumentid, nIndex):
        return self.my_db.GetOrderPrice(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的状态
    def GetOrderStatus(self, instrumentid, nIndex):
        return self.my_db.GetOrderStatus(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托时间
    def GetOrderTime(self, instrumentid, nIndex):
        return self.my_db.GetOrderTime(instrumentid, nIndex)

    # 返回当前公式应用的帐户下当前商品的持仓盈亏
    def GetPositionProfitLoss(self, instrumentid):
        return self.my_db.GetPositionProfitLoss(instrumentid)

    # 返回当前公式应用的帐户下当前商品的买入持仓
    # type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def GetBuyPosition(self, instrumentid,type=0):
        print(f"Enter GetBuyPosition,子进程：（{os.getpid()}）,父进程={os.getppid()}")
        return self.my_db.qry_buyposition(instrumentid,type)

    #返回当前公式应用的帐户下当前商品的买入持仓盈亏
    def GetBuyProfitLoss(self, instrumentid):
        return self.my_db.qry_buyprofitloss(instrumentid)

    # 返回当前公式应用的帐户下当前商品的卖出持仓
    # type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def GetSellPosition(self, instrumentid,type=0):
        return self.my_db.qry_sellposition(instrumentid,type)

    # 返回当前公式应用的帐户下当前商品的卖出持仓盈亏
    def GetSellProfitLoss(self, instrumentid):
        return self.my_db.qry_sellprofitloss(instrumentid)

    # 返回当前公式应用的交易帐户的昨日结存
    def A_PreviousEquity(self, accountIndex):
        return self.my_db.qry_previousequity(accountIndex)

    # 返回当前公式应用的帐户下当前商品的当日买入持仓
    def GetTodayBuyPosition(self, instrumentid):
        return self.my_db.GetTodayBuyPosition(instrumentid)

    # 返回当前公式应用的帐户下当前商品的当日卖出持仓
    def GetTodaySellPosition(self, instrumentid):
        return self.my_db.GetTodaySellPosition(instrumentid)

    def Join(self):

        #放在函数中导入解决两个py文件互相导入问题
        from MarketData import data
        from MarketData import g_settingmap

        print(self.mystatus)
        preReqAccountTime = int(time.time())
        bar_sub = BarSub()
        cnt = 0

        print(f'进入Join函数，子进程：（{os.getpid()}）,父进程={os.getppid()}')

        #根据策略设置得到策略执行代码
        strategy_inst = None #策略代码模块
        try:
            print(f'子进程：（{os.getpid()}）,join:g_strategy_file={g_settingmap["strategy_file"]}')
            strategy_inst = __import__(f'strategy.{g_settingmap["strategy_file"]}', fromlist=[f'{g_settingmap["strategy_file"]}'])
        except ImportError:
            print(f"import {g_settingmap['strategy_file']} error")
        else:
            print(f'strategy_inst={strategy_inst}')

        while True:
            nowtime = int(time.time())
            if self.mystatus == 1 and self.is_first_reqaccount == True:
                #self.my_db.cancle_Yorder() #第一次运行先撤销所有未成交昨日报单

                print("$$$$$$$$$$$$$$$$$begin AfterTrade()")
                self.AfterTrade() #程序开始运行第一次查询资金账户和持仓情况

                #查询所有合约的保证金率及合约信息
                print(f'***data={data}')
                for d in data:
                    instrument_id = d.get_instrumentid()
                    print(f'***qry_marginrate,instrument_id={instrument_id}')
                    time.sleep(0.3)
                    self.qry_marginrate(instrument_id)
                    #time.sleep(0.3)
                    #self.qry_instrument(instrument_id)

                time.sleep(0.5)
                self.qry_instrument('')

                preReqAccountTime = nowtime
            elif self.mystatus == 1 and (nowtime-preReqAccountTime) > 600: #后续每10分钟查询一次资金账户
                time.sleep(0.1)
                self.qry_account()
                time.sleep(0.1)
                preReqAccountTime = nowtime

            if self.has_new_trade == True and (nowtime - self.last_trade_time) >30:
                self.AfterTrade()

            bar = bar_sub.getdata() #从zeromq队列中得到bar
            level = -1

            cnt += 1
            if cnt >10:
                cnt = 0
                print(f'bar={bar}')

            if bar != None and strategy_inst != None:
                if bar["DataType"] == "bardata": #是bar数据
                    print(f'it is bardata***,InstrumentID={bar["InstrumentID"]}')
                    #查找bar对应的data层级
                    for d in data:
                        print(f'd InstrumentID={d.instrumentid}')
                        level += 1
                        if d.is_the_instrument(bar["InstrumentID"]):
                            print("*** OnBar")
                            d.putbar(bar)
                            strategy_inst.OnBar(level)#收到一个bar,执行一次策略
                            break
                elif bar["DataType"] == "tickdata": #是市场tick数据
                    print("it is tickdata!!!")
                    for d in data:
                        if d.is_the_instrument(bar["InstrumentID"]):
                            d.puttickdata(bar)
                            break

                '''if bar["DataType"] == "bardata":
                    print(f'InstrumentID={bar["InstrumentID"]},HighPrice={bar["HighPrice"]}')
                    #self.sell_open(b'SHFE', bar["InstrumentID"], bar["HighPrice"]-10, 5)
                    #time.sleep(3)'''

            time.sleep(0.1)

#定义全局变量
from account_info import my_future_account_info_dict
future_account = my_future_account_info_dict['SimNow']

#不要创建全局对象，否则在主进程中也会创建该对象
#g_ctptrader = CTPTrader.get_instance()
