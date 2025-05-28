#提供策略代码调用的接口
#from CTPTrader import g_ctptrader
from CTPTrader import CTPTrader
from account_info import my_future_account_info_dict

#返回当前公式应用的交易帐户的动态权益,Balance?
def A_CurrentEquity(accountIndex):
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetCurrentEquity()

#返回当前公式应用的交易帐户的可用资金
def A_FreeMargin():
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetFreeMargin()

# 返回当前公式应用的交易帐户的当日入金。
def A_TodayDeposit(accountIndex):
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetTodayDeposit()

# 返回当前公式应用的交易帐户的当日出金。
def A_TodayDrawing( accountIndex):
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetTodayDrawing()

# 返回当前公式应用的交易帐户的资金冻结。
def A_TotalFreeze(accountIndex):
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetTotalFreeze()

# 返回当前公式应用的交易帐户的持仓保证金。
def A_TotalMargin(accountIndex):
    future_account = my_future_account_info_dict['SimNow']
    return future_account.GetTotalMargin()

#返回当前公式应用的交易帐户的昨日结存。
def A_PreviousEquity(accountIndex):
    p_ctptrader = CTPTrader.get_instance()
    return p_ctptrader.A_PreviousEquity(accountIndex)

