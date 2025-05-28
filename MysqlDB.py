import os
#import threading

from dbutils.pooled_db import PooledDB
from DBApi import DBApi
from datetime import datetime
import pymysql
from util.mytime import CurrentDate
from Logger import Logger

db_pool = PooledDB(
    creator=pymysql,
    maxconnections=3,
    mincached=2,
    maxcached=1,
    blocking=True,
    maxusage=None,
    setsession=[],
    host='localhost',
    port=3306,
    user='root',
    password='123456',
    database='traderdb',
    charset='utf8'
)

g_logger = Logger("Trade","Trade.log")

class MysqlDB(DBApi):
    def __init__(self):
        #self.lock = threading.Lock()
        pass

    '''def set_dbpool(self,dbpool:PooledDB):
        self.dbpool = dbpool'''

    #查询策略主表
    def qry_enable_strategy(self):
        #self.lock.acquire()
        sql = "select * from strategy where enable='1' "
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        conn.close()
        #self.lock.release()
        return data

    #查询策略数据源
    def qry_strategy_ds(self,strategyid):
        #self.lock.acquire()
        sql = f'select * from strategy_ds where strategyid={strategyid} '
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        conn.close()
        #self.lock.release()
        return data

    def insert_tradaccount(self, pTradingAccount):
        print(f'insert_tradaccount:MysqlDB.MysqlDB.self={id(self)}')
        #self.lock.acquire()
        sql = ("insert into trading_account values(DEFAULT,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        print(f'2 db_pool={id(db_pool)}')
        conn = db_pool.connection()
        cursor = conn.cursor()

        #pTradingAccount是包含资金账号的字典，其顺序和数据库表字段的顺序一致，所以直接将其所有值转成元组
        cursor.execute(sql, tuple(pTradingAccount.values()))
        conn.commit()

        cursor.close()
        conn.close()
        #self.lock.release()

    def is_exist_account(self):
        #self.lock.acquire()
        #today = str(CurrentDate())
        sql = f'select count(*) from trading_account '
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        if data[0] != None and data[0] > 0:
            return True
        else:
            return False

    def update_tradaccount(self, pTradingAccount):
        print(f'update_tradaccount:db_pool={id(db_pool)},MysqlDB.self={id(self)}')
        #self.lock.acquire()
        conn = db_pool.connection()
        #today = str(CurrentDate())
        key_list = list(pTradingAccount.keys()) #取账号字典中的key转成列表
        val_list = list(pTradingAccount.values()) #取账号字典中的值转成列表
        '''head = key_list[:2] #取前两个key，即BrokerID和AccountID
        tail = key_list[2:] #取除BrokerID和AccountID之外的其它key
        key_list = tail + head #将BrokerID和AccountID放到列表最后组成新列表

        head = val_list[:2] #取前两个值，即BrokerID和AccountID对应的值
        tail = val_list[2:] #取除BrokerID和AccountID值之外的其它值
        val_list = tail + head #将BrokerID和AccountID两个的值放到列表最后组成新列表'''

        sql = 'update trading_account set '
        cnt = 2

        #把所有的key和值组成set对
        while cnt < len(key_list): #BrokerID和AccountID不处理
            val = val_list[cnt]
            is_str = False
            if isinstance(val, bytes): #判断值是否是字节类型
                try:
                    print(f'val={val}')
                    val = val.decode('utf-8') #是字节类型则转成字符串
                except UnicodeDecodeError:
                    val = ""
                is_str = True

            if cnt == len(key_list) -1:
                if is_str:
                    sub = f'{key_list[cnt]}="{val}" ' #值是字符串要加上双引号
                else:
                    sub = f'{key_list[cnt]}={val} '
            else:
                if is_str:
                    sub = f'{key_list[cnt]}="{val}", ' #值是字符串要加上双引号
                else:
                    sub = f'{key_list[cnt]}={val}, '
            sql += sub
            cnt += 1

        #最后加上BrokerID和AccountID作为update的条件
        sql += (f' where {key_list[0]}=\"{val_list[0].decode("utf-8")}\" and '
                f' {key_list[1]}=\"{val_list[1].decode("utf-8")}\" ' )

        #print("!!!update sql:")
        #print(sql)

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        cursor.close()
        conn.close()
        #self.lock.release()

    def qry_order_byid(self,orderid):
        #self.lock.acquire()
        sql = f"select * from order_inf where OrderID={orderid}"
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        return data

    def insert_orderinfo(self, pOrderInfo):
        if len(dict(pOrderInfo)) == 0 :
            return

        #self.lock.acquire()
        conn = db_pool.connection()
        #print("enter insert_orderinfo")
        sql = "insert into order_inf(FrontID,SessionID,OrderRef,InstrumentID,Direction,OffsetFlag,LimitPrice,VolumeTotalOriginal,\
        VolumeTraded,VolumeTotal,TradingDay,InsertDate,InsertTime,OrderSubmitStatus,OrderStatus,OrderPriceType,CancelTime,\
        ContingentCondition,ForceCloseReason,RequestID,OrderLocalID,NotifySequence,SettlementID,ExchangeID,OrderSysID,\
        SequenceNo) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor = conn.cursor()

        cursor.execute(sql, (pOrderInfo["FrontID"],
                             pOrderInfo["SessionID"],
                             pOrderInfo["OrderRef"].decode("utf-8"),
                             pOrderInfo["InstrumentID"].decode("utf-8"),
                             pOrderInfo["Direction"].decode("utf-8"),
                             pOrderInfo["CombOffsetFlag"].decode("utf-8"),
                             pOrderInfo["LimitPrice"],
                             pOrderInfo["VolumeTotalOriginal"],
                             pOrderInfo["VolumeTraded"],
                             pOrderInfo["VolumeTotal"],
                             pOrderInfo["TradingDay"].decode("utf-8"),
                             pOrderInfo["InsertDate"].decode("utf-8"),
                             pOrderInfo["InsertTime"].decode("utf-8"),
                             pOrderInfo["OrderSubmitStatus"].decode("utf-8"),
                             pOrderInfo["OrderStatus"].decode("utf-8"),
                             pOrderInfo["OrderPriceType"].decode("utf-8"),
                             pOrderInfo["CancelTime"].decode("utf-8"),
                             pOrderInfo["ContingentCondition"].decode("utf-8"),
                             pOrderInfo["ForceCloseReason"].decode("utf-8"),
                             pOrderInfo["RequestID"],
                             pOrderInfo["OrderLocalID"].decode("utf-8"),
                             pOrderInfo["NotifySequence"],
                             pOrderInfo["SettlementID"],
                             pOrderInfo["ExchangeID"].decode("utf-8"),
                             pOrderInfo["OrderSysID"].decode("utf-8"),
                             pOrderInfo["SequenceNo"]))
        conn.commit()

        cursor.close()
        conn.close()
        #self.lock.release()
        #print(f'insert_orderinfo sql={sql}')

    #撤销所有未成交未撤销的所有昨日报单（昨日未成交报单在交易所自动撤销且不会发报单信息过来，因此程序启动时先处理昨日报单）
    def cancle_Yorder(self):
        today = str(CurrentDate())
        sql = (f'update order_inf set OrderStatus="5" where OrderStatus !="0" and OrderStatus !="5" '
               f'and OrderStatus !="1" and OrderStatus !="2" and TradingDay !="{today}"')

        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        cursor.close()
        conn.close()
        #self.lock.release()

    #查询报单状态，返回：0-报单不存在，1-报单存在且未完成，2-报单已完成或者已撤单
    def order_status(self,pOrderInfo):
        #print(f'order_status:MysqlDB.self={id(self)}')
        if len(dict(pOrderInfo)) == 0 :
            return False

        #self.lock.acquire()
        order_ref = pOrderInfo["OrderRef"].decode("utf-8")
        #print(f'order_status:order_ref={order_ref},dbpool={db_pool}')

        conn = db_pool.connection()

        #由FrontID、SessionID、OrderRef组合确定报单，也可以由ExchangeID、OrderSysID组合确定报单
        sql = (f'select * from order_inf where FrontID={pOrderInfo["FrontID"]} and '
            f'SessionID={pOrderInfo["SessionID"]} and OrderRef="{order_ref}"')

        #print(f'is_exist_order sql={sql}')
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0
        else:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            #print(f'order_status data ={data}')
            if data["OrderStatus"] == "0" or data["OrderStatus"] == "5":
                #print("order_status return 2")
                return 2
            else:
                #print("order_status return 1")
                return 1

    #根据合约查询没有完成交易的报单
    def qry_uncomplete_order(self,instrumentid):
        #self.lock.acquire()
        sql = (f'select * from order_inf where InstrumentID="{instrumentid}" and OrderStatus !="0" and OrderStatus !="5" ')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchall()

        cursor.close()
        conn.close()
        #self.lock.release()
        return data

    #回当前公式应用的帐户下当前商品的最后一个未成交委托单索引，按输入参数为条件
    #BuyOrSell:1-买，2-卖
    #EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def GetLastOpenOrderIndex(self,instrumentid,BuyOrSell,EntryOrExit):
        buy_sell = BuyOrSell -1
        OffsetFlag = "0"
        if EntryOrExit == 1:
            OffsetFlag = "0"
        elif EntryOrExit == 5:
            OffsetFlag = "3"
        elif EntryOrExit == 6:
            OffsetFlag = "4"
        else:
            OffsetFlag = "1"

        #self.lock.acquire()

        sql = (f'select * from order_inf where InstrumentID="{instrumentid}" and OrderStatus !="0" and OrderStatus !="5" '
               f' and Direction="{str(buy_sell)}" and OffsetFlag="{OffsetFlag}" order by OrderID DESC LIMIT 1')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchone()
        row_cnt = cursor.rowcount

        cursor.close()
        conn.close()
        #self.lock.release()
        if row_cnt == 0:
            return 0
        else:
            return data["OrderID"]

    # 返回指定帐户下当前商品的最后一个当日委托单索引，按输入参数为条件
    # BuyOrSell:1-买，2-卖
    # EntryOrExit：1-开仓，5-平今仓，6-平昨，7-平仓
    def GetLastOrderIndex(self, instrumentid, BuyOrSell, EntryOrExit):
        buy_sell = BuyOrSell - 1
        OffsetFlag = "0"
        if EntryOrExit == 1:
            OffsetFlag = "0"
        elif EntryOrExit == 5:
            OffsetFlag = "3"
        elif EntryOrExit == 6:
            OffsetFlag = "4"
        else:
            OffsetFlag = "1"

        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day
        str_month = "{:02d}".format(month)
        str_day = "{:02d}".format(day)
        str_date = str(year) + str_month + str_day

        #self.lock.acquire()

        sql = (f'select * from order_inf where InstrumentID="{instrumentid}" and InsertDate="{str_date}" '
            f' and Direction="{str(buy_sell)}" and OffsetFlag="{OffsetFlag}" order by OrderID DESC LIMIT 1')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchone()
        row_cnt = cursor.rowcount

        cursor.close()
        conn.close()
        #self.lock.release()
        if row_cnt == 0:
            return 0
        else:
            return data["OrderID"]

    # 返回当前公式应用的帐户下当前商品的未成交委托单数量
    def GetOpenOrderCount(self, instrumentid):
        #self.lock.acquire()
        sql = (f'select count(*) from order_inf where InstrumentID="{instrumentid}" and OrderStatus !="0" and OrderStatus !="5"')
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        if data[0] == None:
            return 0
        else:
            return data[0]

    #返回当前公式应用的帐户下当前商品的当日委托单数量
    def GetOrderCount(self, instrumentid):
        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day
        str_month = "{:02d}".format(month)
        str_day = "{:02d}".format(day)
        str_date = str(year) + str_month + str_day

        #self.lock.acquire()

        sql = (f'select count(*) from order_inf where InstrumentID="{instrumentid}" and InsertDate="{str_date}" ')
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        if data[0] == None:
            return 0
        else:
            return data[0]

    #返回当前公式应用的帐户下当前商品的某个未成交委托单
    def GetOpenOrder(self, instrumentid,nIndex):
        #self.lock.acquire()
        sql = (f'select * from order_inf where InstrumentID="{instrumentid}" '
               f'and OrderStatus !="0" and OrderStatus !="5" order by OrderID DESC ')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchall()
        row_cnt = cursor.rowcount

        cursor.close()
        conn.close()
        #self.lock.release()

        if nIndex >= row_cnt:
            return None
        else:
            return data[nIndex]

    # 返回当前公式应用的帐户下当前商品的某个委托单
    def GetOrder(self, instrumentid, nIndex):
        #self.lock.acquire()
        sql = (f'select * from order_inf where InstrumentID="{instrumentid}" order by OrderID DESC')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchall()
        row_cnt = cursor.rowcount

        cursor.close()
        conn.close()
        #self.lock.release()

        if nIndex >= row_cnt:
            return None
        else:
            return data[nIndex]

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的买卖类型
    #nIndex=0表示最后提交的报单
    def GetOpenOrderBuyOrSell(self, instrumentid,nIndex):
        one_data = self.GetOpenOrder(instrumentid,nIndex)
        if one_data is None:
            return -1
        else:
            return int( one_data['Direction'] )

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的开平仓状态
    # nIndex=0表示最后提交的报单
    def GetOpenOrderEntryOrExit(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['OffsetFlag'])

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的成交数量
    # nIndex=0表示最后提交的报单
    def GetOpenOrderFilledLot(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['VolumeTraded'])

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的成交价格
    # nIndex=0表示最后提交的报单
    def GetOpenOrderFilledPrice(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            one_trade_inf = self.GetLastTradeByOrderid(one_data['OrderID'])
            if one_trade_inf is None:
                return -1
            else:
                return float(one_trade_inf['Price'])

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的委托数量
    # nIndex=0表示最后提交的报单
    def GetOpenOrderLot(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['VolumeTotalOriginal'])

    #返回当前公式应用的帐户下当前商品的某个未成交委托单的委托价格
    # nIndex=0表示最后提交的报单
    def GetOpenOrderPrice(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['LimitPrice'])

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的委托价格
    # nIndex=0表示最后提交的报单
    def GetOpenOrderStatus(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['OrderStatus'])

    # 返回当前公式应用的帐户下当前商品的某个未成交委托单的委托时间
    # nIndex=0表示最后提交的报单
    def GetOpenOrderTime(self, instrumentid, nIndex):
        one_data = self.GetOpenOrder(instrumentid, nIndex)
        if one_data is None:
            return ""
        else:
            return one_data['InsertTime']

    # 返回当前公式应用的帐户下当前商品的某个委托单的买卖类型
    # nIndex=0表示最后提交的报单
    def GetOrderBuyOrSell(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['Direction'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的撤单数量
    # nIndex=0表示最后提交的报单
    def GetOrderCanceledLot(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None or one_data['OrderStatus'] != "5" :
            return 0
        else:
            return int(one_data['VolumeTotal'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的开平仓状态
    # nIndex=0表示最后提交的报单
    def GetOrderEntryOrExit(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['OffsetFlag'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交数量
    # nIndex=0表示最后提交的报单
    def GetOrderFilledLot(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['VolumeTraded'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的成交价格
    # nIndex=0表示最后提交的报单
    def GetOrderFilledPrice(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            one_trade_inf = self.GetLastTradeByOrderid(one_data['OrderID'])
            if one_trade_inf is None:
                return -1
            else:
                return float(one_trade_inf['Price'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托数量
    # nIndex=0表示最后提交的报单
    def GetOrderLot(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['VolumeTotalOriginal'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托价格
    # nIndex=0表示最后提交的报单
    def GetOrderPrice(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['LimitPrice'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的状态
    # nIndex=0表示最后提交的报单
    def GetOrderStatus(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['OrderStatus'])

    # 返回当前公式应用的帐户下当前商品的某个委托单的委托时间
    # nIndex=0表示最后提交的报单
    def GetOrderTime(self, instrumentid, nIndex):
        one_data = self.GetOrder(instrumentid, nIndex)
        if one_data is None:
            return -1
        else:
            return int(one_data['InsertTime'])
    def update_orderinfo(self, pOrderInfo):
        if len(dict(pOrderInfo)) == 0 :
            return

        #self.lock.acquire()
        print("enter update_orderinfo")
        sql = "update order_inf set VolumeTraded=%s,VolumeTotal=%s,OrderSubmitStatus=%s,OrderStatus=%s,CancelTime=%s,\
               ForceCloseReason=%s,RequestID=%s, OrderSysID=%s where FrontID=%s and OrderRef=%s and SessionID=%s "

        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql, (pOrderInfo["VolumeTraded"],
                             pOrderInfo["VolumeTotal"],
                             pOrderInfo["OrderSubmitStatus"].decode("utf-8"),
                             pOrderInfo["OrderStatus"].decode("utf-8"),
                             pOrderInfo["CancelTime"].decode("utf-8"),
                             pOrderInfo["ForceCloseReason"].decode("utf-8"),
                             pOrderInfo["RequestID"],
                             pOrderInfo["OrderSysID"].decode("utf-8"),
                             pOrderInfo["FrontID"],
                             pOrderInfo["OrderRef"].decode("utf-8"),
                             pOrderInfo["SessionID"]))
        conn.commit()
        cursor.close()
        conn.close()
        #self.lock.release()

    #查询数据库交易记录是否存在
    def is_exist_tradeinf(self,trade_key):
        sql = f'select count(*) from trade_inf where \
        ExchangeID="{trade_key["ExchangeID"].decode("utf-8")}" and \
        TradeID="{trade_key["TradeID"].decode("utf-8")}" and \
        OrderSysID="{trade_key["OrderSysID"].decode("utf-8")}" and \
        Direction="{trade_key["Direction"].decode("utf-8")}"'

        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        if data[0] != None and data[0] > 0:
            return True
        else:
            return False

    #保存成交记录
    def insert_tradeinfo(self, pTrade):
        if len(dict(pTrade)) == 0 :
            return
        #先查询成交对应的报单ID
        #self.lock.acquire()
        sql = f'select OrderID from order_inf where ExchangeID="{pTrade["ExchangeID"].decode("utf-8")}" and \
                    OrderSysID="{pTrade["OrderSysID"].decode("utf-8")}" '
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount < 1:
            print(f'insert_tradeinfo no finde orderid,ExchangeID="{pTrade["ExchangeID"].decode("utf-8")}" and \
                    OrderSysID="{pTrade["OrderSysID"].decode("utf-8")}"')
            cursor.close()
            conn.close()
            #self.lock.release()
            return
        data = cursor.fetchone()

        sql = "insert into trade_inf(OrderID,InstrumentID,ExchangeID,TradeID,OrderSysID,Direction,OffsetFlag,HedgeFlag,Volume,Price,\
        TradeDate,TradeTime,TradeType) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql, (data[0],
                             pTrade["InstrumentID"].decode("utf-8"),
                             pTrade["ExchangeID"].decode("utf-8"),
                             pTrade["TradeID"].decode("utf-8"),
                             pTrade["OrderSysID"].decode("utf-8"),
                             pTrade["Direction"].decode("utf-8"),
                             pTrade["OffsetFlag"].decode("utf-8"),
                             pTrade["HedgeFlag"].decode("utf-8"),
                             pTrade["Volume"],
                             pTrade["Price"],
                             pTrade["TradeDate"].decode("utf-8"),
                             pTrade["TradeTime"].decode("utf-8"),
                             pTrade["TradeType"].decode("utf-8")
                             ))
        conn.commit()
        cursor.close()
        conn.close()
        #self.lock.release()

    #根据报单ID查找最后一条成交记录
    def GetLastTradeByOrderid(self,orderid):
        #self.lock.acquire()
        sql = (f'select * from trade_inf where OrderID={orderid} ORDER BY TradeInfoID DESC LIMIT  1')
        conn = db_pool.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()

        return data

    #清除持仓表
    def clear_investor_position(self):
        sql = "delete from investor_position"
        conn = db_pool.connection()
        cursor = conn.cursor()

        # 重新整理值的顺序插入数据库
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def insert_investor_position(self, pInvestorPosition):
        sql = ("insert into investor_position values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

        #除了键值，数据库字段的顺序与pInvestorPosition字典一致
        key_list = list(pInvestorPosition.keys())  # 取账号字典中的key转成列表
        val_list = list(pInvestorPosition.values())  # 取账号字典中的值转成列表

        InstrumentID_idx = key_list.index('InstrumentID')
        InstrumentID = val_list.pop(InstrumentID_idx) #取出InstrumentID的值并从列表中删除
        key_list.pop(InstrumentID_idx)

        BrokerID_idx = key_list.index('BrokerID')
        BrokerID = val_list.pop(BrokerID_idx)  # 取出BrokerID的值并从列表中删除
        key_list.pop(BrokerID_idx)

        InvestorID_idx = key_list.index('InvestorID')
        InvestorID = val_list.pop(InvestorID_idx)  # 取出InvestorID的值并从列表中删除
        key_list.pop(InvestorID_idx)

        PosiDirection_idx = key_list.index('PosiDirection')
        PosiDirection = val_list.pop(PosiDirection_idx)  # 取出PosiDirection的值并从列表中删除
        key_list.pop(PosiDirection_idx)

        HedgeFlag_idx = key_list.index('HedgeFlag')
        HedgeFlag = val_list.pop(HedgeFlag_idx)  # 取出HedgeFlag的值并从列表中删除
        key_list.pop(HedgeFlag_idx)

        PositionDate_idx = key_list.index('PositionDate')
        PositionDate = val_list.pop(PositionDate_idx)  # 取出PositionDate的值并从列表中删除
        key_list.pop(PositionDate_idx)

        #这几个值作为键值放在最前面
        val_list.insert(0, PositionDate)
        val_list.insert(0, HedgeFlag)
        val_list.insert(0, PosiDirection)
        val_list.insert(0, InvestorID)
        val_list.insert(0, BrokerID)
        val_list.insert(0, InstrumentID)

        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()

        #重新整理值的顺序插入数据库
        cursor.execute(sql, tuple(val_list))
        conn.commit()
        cursor.close()
        conn.close()
        #self.lock.release()

    def is_exist_investor_position(self,pInvestorPosition):
        #self.lock.acquire()
        sql = (f'select count(*) from investor_position where InstrumentID="{pInvestorPosition["InstrumentID"].decode("utf-8")}" '
               f'and BrokerID="{pInvestorPosition["BrokerID"].decode("utf-8")}" and '
               f'InvestorID="{pInvestorPosition["InvestorID"].decode("utf-8")}" and '
               f'PosiDirection="{pInvestorPosition["PosiDirection"].decode("utf-8")}" and '
               f'HedgeFlag="{pInvestorPosition["HedgeFlag"].decode("utf-8")}" and '
               f'PositionDate="{pInvestorPosition["PositionDate"].decode("utf-8")}" ')
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        #self.lock.release()
        if data[0] != None and data[0] > 0:
            return True
        else:
            return False

    def update_investor_position(self, pInvestorPosition):
        key_list = list(pInvestorPosition.keys())  # 取账号字典中的key转成列表
        val_list = list(pInvestorPosition.values())  # 取账号字典中的值转成列表

        InstrumentID_idx = key_list.index('InstrumentID')
        InstrumentID = val_list.pop(InstrumentID_idx)  # 取出InstrumentID的值并从列表中删除
        key_list.pop(InstrumentID_idx)

        BrokerID_idx = key_list.index('BrokerID')
        BrokerID = val_list.pop(BrokerID_idx)  # 取出BrokerID的值并从列表中删除
        key_list.pop(BrokerID_idx)

        InvestorID_idx = key_list.index('InvestorID')
        InvestorID = val_list.pop(InvestorID_idx)  # 取出InvestorID的值并从列表中删除
        key_list.pop(InvestorID_idx)

        PosiDirection_idx = key_list.index('PosiDirection')
        PosiDirection = val_list.pop(PosiDirection_idx)  # 取出PosiDirection的值并从列表中删除
        key_list.pop(PosiDirection_idx)

        HedgeFlag_idx = key_list.index('HedgeFlag')
        HedgeFlag = val_list.pop(HedgeFlag_idx)  # 取出HedgeFlag的值并从列表中删除
        key_list.pop(HedgeFlag_idx)

        PositionDate_idx = key_list.index('PositionDate')
        PositionDate = val_list.pop(PositionDate_idx)  # 取出PositionDate的值并从列表中删除
        key_list.pop(PositionDate_idx)

        sql = 'update investor_position set '
        cnt = 0

        # 把所有的key和值组成set对
        while cnt < len(key_list):
            val = val_list[cnt]
            is_str = False
            if isinstance(val, bytes):  # 判断值是否是字节类型
                val = val.decode('utf-8')  # 是字节类型则转成字符串
                is_str = True

            if cnt == len(key_list) - 1:
                if is_str:
                    sub = f'{key_list[cnt]}="{val}" '  # 值是字符串要加上双引号
                else:
                    sub = f'{key_list[cnt]}={val} '
            else:
                if is_str:
                    sub = f'{key_list[cnt]}="{val}", '  # 值是字符串要加上双引号
                else:
                    sub = f'{key_list[cnt]}={val}, '
            sql += sub
            cnt += 1

        # 最后加KEY作为update的条件
        sql += (f' where InstrumentID="{InstrumentID.decode("utf-8")}" '
               f'and BrokerID="{BrokerID.decode("utf-8")}" and '
               f'InvestorID="{InvestorID.decode("utf-8")}" and '
               f'PosiDirection="{PosiDirection.decode("utf-8")}" and '
               f'HedgeFlag="{HedgeFlag.decode("utf-8")}" and '
               f'PositionDate="{PositionDate.decode("utf-8")}" ')

        print(f'update_investor_position update sql:{sql}')

        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        #self.lock.release()

    # 返回当前公式应用的帐户下当前商品的买入持仓
    #type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def qry_buyposition(self, instrumentid,type=0):
        print(f'qry_buyposition:MysqlDB.self={id(self)}')
        if db_pool is None:
            print(f'qry_buyposition:dbpool is None,MysqlDB.self={id(self)}')
            return

        #self.lock.acquire()
        print(f"Enter qry_buyposition,子进程：（{os.getpid()}）,父进程={os.getppid()}，dbpool={id(db_pool)}")

        sql = (f'select sum(Position) as totposition from investor_position where PosiDirection="2" '
               f' and InstrumentID="{instrumentid}" ')

        if type == 1:
            sql += 'and PositionDate ="1" '
        elif type == 2:
            sql += 'and PositionDate ="2" '

        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的买入持仓盈亏
    def qry_buyprofitloss(self, instrumentid):
        #self.lock.acquire()
        sql = (f'select sum(PositionProfit) as totlosss from investor_position where PosiDirection="2" and '
               f' InstrumentID="{instrumentid}" ')
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的卖出持仓
    # type：查询类型：0-全部，1-今日持仓，2-昨日持仓
    def qry_sellposition(self, instrumentid,type=0):
        #self.lock.acquire()
        sql = (f'select sum(Position) as totposition from investor_position where PosiDirection="3" '
               f' and InstrumentID="{instrumentid}" ')

        if type == 1:
            sql += 'and PositionDate ="1" '
        elif type == 2:
            sql += 'and PositionDate ="2" '

        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的卖出持仓盈亏
    def qry_sellprofitloss(self, instrumentid):
        sql = (f'select sum(PositionProfit) as totlosss from investor_position where PosiDirection="3" and '
               f' InstrumentID="{instrumentid}" ')
        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的持仓盈亏
    def GetPositionProfitLoss(self, instrumentid):
        #self.lock.acquire()
        sql = f'select sum(PositionProfit) as totlosss from investor_position where InstrumentID="{instrumentid}" '
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的当日买入持仓
    def GetTodayBuyPosition(self, instrumentid):
        today = str(CurrentDate())
        #self.lock.acquire()
        sql = (f'select sum(TodayPosition) as totposition from investor_position where PosiDirection="2" '
                f'and PositionDate="1" and InstrumentID="{instrumentid}" and TradingDay="{today}" ')
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的帐户下当前商品的当日卖出持仓
    def GetTodaySellPosition(self, instrumentid):
        today = str(CurrentDate())
        sql = (f'select sum(TodayPosition) as totposition from investor_position where PosiDirection="3" '
                f'and PositionDate="1" and InstrumentID="{instrumentid}" and TradingDay="{today}" ')
        #self.lock.acquire()
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.rowcount > 0:
            data = cursor.fetchone()
            cursor.close()
            conn.close()
            #self.lock.release()
            if data[0] == None:
                return 0
            else:
                return data[0]
        else:
            cursor.close()
            conn.close()
            #self.lock.release()
            return 0

    # 返回当前公式应用的交易帐户的昨日结存 #TODO
    def qry_previousequity(self, instrumentid):
        return 0