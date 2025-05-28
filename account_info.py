import ast
import base64
import configparser
import copy

from gmssl import sm4

from util.mytime import CurrentDate

# encoding:utf-8
'''BASE_LOCATION = "."  # 根目录地址
MD_LOCATION = BASE_LOCATION + "\MarketData"  # 行情数据地址
TD_LOCATION = BASE_LOCATION + "\TradingData"  # 交易数据地址
SD_LOCATION = BASE_LOCATION + "\StrategyData"  # 策略数据地址'''
BASE_LOCATION = "d:/flow"
MD_LOCATION = BASE_LOCATION
TD_LOCATION = BASE_LOCATION

SM4_KEY = b"xxxxxxxxxxabcdef" #SM4密钥，必须是16字节


class FutureAccountInfo:
    def __init__(self, accounttype,md_page_dir=MD_LOCATION, td_page_dir=TD_LOCATION):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.broker_id = config[accounttype]['BrokerID'].encode('utf-8')
        self.server_dict = ast.literal_eval(config[accounttype]['server_dict'])  # 服务器地址
        self.reserve_server_dict = ast.literal_eval(config[accounttype]['reserve_server_dict'])  # 备用服务器地址
        self.investor_id = config[accounttype]['InvestorID'].encode('utf-8')  # 账户
        self.password = config[accounttype]['Password'].encode('utf-8')  # 密码
        self.app_id = config[accounttype]['AppID'].encode('utf-8')  # 认证使用AppID
        self.auth_code = config[accounttype]['AuthCode'].encode('utf-8')  # 认证使用授权码
        self.md_page_dir = md_page_dir  # MdApi流文件存储地址，默认MD_LOCATION
        self.td_page_dir = td_page_dir  # TraderApi流文件存储地址，默认TD_LOCATION

        if accounttype == "Real":
            # 解密
            sm4_cipher = sm4.CryptSM4()
            sm4_cipher.set_key(SM4_KEY, sm4.SM4_DECRYPT)
            self.investor_id = sm4_cipher.crypt_ecb(base64.b64decode(self.investor_id)).decode()
            self.password = sm4_cipher.crypt_ecb(base64.b64decode(self.password)).decode()
            self.investor_id = self.investor_id.encode('utf-8')
            self.password = self.password.encode('utf-8')

        self.trading_account = {} #资金账号信息

    #设置资金账号信息
    def set_trading_account(self,pTradingAccount):
       ''' today = str(CurrentDate())
        print(f'enter set_trading_account,today={today},pTradingAccount={pTradingAccount}')
        if today == pTradingAccount["TradingDay"].decode("utf-8"):'''
       if 1:
            self.trading_account = copy.deepcopy(pTradingAccount)

    # 返回当前公式应用的交易帐户的静态权益
    def get_static_equity(self):
        return self.trading_account["PreBalance"] + self.trading_account["Deposit"] - self.trading_account["Withdraw"]

    #返回当前公式应用的交易帐户的动态权益,Balance?
    def GetCurrentEquity(self):
        return (self.get_static_equity() + self.trading_account["PositionProfit"] + self.trading_account["CloseProfit"]
                - self.trading_account["Commission"])

    #返回当前公式应用的交易帐户的可用资金
    def GetFreeMargin(self):
        print(f'self.trading_account={self.trading_account}')
        return self.trading_account["Available"]

    # 返回当前公式应用的交易帐户的当日入金。
    def GetTodayDeposit(self):
        return self.trading_account["Deposit"]

    # 返回当前公式应用的交易帐户的当日出金。
    def GetTodayDrawing(self):
        return self.trading_account["Withdraw"]

    # 返回当前公式应用的交易帐户的资金冻结
    def GetTotalFreeze(self):
        return self.trading_account["FrozenCash"]

    # 返回当前公式应用的交易帐户的持仓保证金
    def GetTotalMargin(self):
        return self.trading_account["FrozenMargin"]


my_future_account_info_dict = {
    # 交易时间测试
    'SimNow': FutureAccountInfo('SimNow'),
    'Real': FutureAccountInfo('Real'),
}
