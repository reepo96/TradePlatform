from MarketData import data  #合约相关函数、属性
from InterFace import * #账户相关查询函数
from util.mymath import * #数学相关函数
#from CTPTrader import g_ctptrader
from MysqlDB import g_logger

INVALIDNUM = -123456.78

def XAverage():
    return (0.03,0.03,0.03,0.03,0.03,0.03)

def ContractUnit():
    return 5

def BigPointValue():
    return 1

def HighestFC(vals,v_len):
    idx =0
    max_val = -999.0
    while idx<len(vals) and idx <v_len :
        if max_val < vals[idx]:
            max_val = vals[idx]
        idx += 1

    return max_val

def LowestFC(vals,v_len):
    idx = 0
    min_val = 9999999.0
    while idx < len(vals) and idx < v_len:
        if min_val > vals[idx]:
            min_val = vals[idx]

        idx += 1

    return min_val

def min(val1 ,val2):
    if val1 < val2:
        return val1
    else:
        return val2

def max(val1, val2):
    if val1 > val2:
        return val1
    else:
        return val2

#执行策略
def OnBar(level):
    print(">>>>>Enter Turtle Onbar")
    myEntryPrice = 0.0
    preEntryPrice = 0.0
    PreBreakoutFailure = True
    RiskRatio = 1.0
    boLength = 20
    fsLength = 55
    teLength = 10

    if data[level].BarStatus() == 0:
        preEntryPrice = INVALIDNUM
        PreBreakoutFailure = False

    MinPoint = data[level].MinMove * data[level].PriceScale
    AvgTR = XAverage()
    N = AvgTR[1]
    TotalEquity = A_FreeMargin() + A_TotalMargin(0)
    TurtleUnits = (TotalEquity * RiskRatio / 100) / (N * ContractUnit() * BigPointValue())
    TurtleUnits = IntPart(TurtleUnits)   #对小数取整
    TurtleUnits = 1 #just for test
    DonchianHi = HighestFC(data[level].High[1:], boLength)
    DonchianLo = LowestFC(data[level].Low[1:], boLength)
    fsDonchianHi = HighestFC(data[level].High[1:], fsLength)
    fsDonchianLo = LowestFC(data[level].Low[1:], fsLength)
    ExitHighestPrice = HighestFC(data[level].High[1:], teLength)
    ExitLowestPrice = LowestFC(data[level].Low[1:], teLength)

    print("1")

    #If(MarketPosition == 0 & & ((!LastProfitableTradeFilter) Or (PreBreakoutFailure)))
    if 1:
        if data[level].High[0] >DonchianHi and TurtleUnits >= 1 : #突破开仓
            myEntryPrice = min(data[level].High[0], DonchianHi + MinPoint)
            myEntryPrice = data[level].Open[0] if myEntryPrice < data[level].Open[0] else myEntryPrice
            preEntryPrice = myEntryPrice
            print("Turtle buy1")
            data[level].Buy(TurtleUnits,myEntryPrice)
        if data[level].Low[0] > DonchianLo and TurtleUnits >= 1:
            myEntryPrice = max(data[level].Low[0], DonchianLo - MinPoint)
            myEntryPrice = data[level].Open[0] if myEntryPrice > data[level].Open[0] else myEntryPrice
            print("Turtle sell1")
            data[level].Sell(TurtleUnits, myEntryPrice)

    # 长周期突破开仓
    print("2")
    #If(MarketPosition == 0)
    if 1:
        if data[level].High[0] > fsDonchianHi and TurtleUnits >= 1:
            myEntryPrice = min(data[level].High[0], fsDonchianHi + MinPoint)
            myEntryPrice = data[level].Open[0] if myEntryPrice < data[level].Open[0] else myEntryPrice
            preEntryPrice = myEntryPrice
            print("Turtle buy2")
            data[level].Buy(TurtleUnits, myEntryPrice)
        if data[level].Low[0] > fsDonchianLo and TurtleUnits >= 1:
            myEntryPrice = max(data[level].Low[0], fsDonchianLo - MinPoint)
            myEntryPrice = data[level].Open[0] if myEntryPrice > data[level].Open[0] else myEntryPrice
            print("Turtle sell2")
            data[level].Sell(TurtleUnits, myEntryPrice)

    #If(MarketPosition == 1) // 有多仓的情况
    print("3")
    if data[level].A_BuyPosition(0) > 0:
        if data[level].Low[0] < ExitLowestPrice:
            myExitPrice = max(data[level].Low[0], ExitLowestPrice - MinPoint)
            myExitPrice = data[level].Open[0] if myExitPrice > data[level].Open[0] else myExitPrice
            print("Turtle sell3")
            data[level].Sell(0, myExitPrice) #全部平仓
        else:
            if preEntryPrice!=INVALIDNUM and TurtleUnits >= 1:
                if data[level].Open[0] >= preEntryPrice + 0.5*N:
                    myEntryPrice = data[level].Open[0]
                    preEntryPrice = myEntryPrice
                    print("Turtle buy3")
                    data[level].Buy(TurtleUnits, myEntryPrice)

                #以最高价为标准，判断能进行几次增仓
                '''while data[level].High[0] >= preEntryPrice + 0.5*N:
                    myEntryPrice = preEntryPrice + 0.5 * N
                    preEntryPrice = myEntryPrice
                    print("Turtle buy4")
                    data[level].Buy(TurtleUnits, myEntryPrice)'''

            if data[level].Low[0] <= preEntryPrice - 2 * N: #止损指令
                myExitPrice = preEntryPrice - 2 * N
                myExitPrice = data[level].Open[0] if myExitPrice > data[level].Open[0] else myExitPrice
                print("Turtle sell4")
                data[level].Sell(0, myExitPrice)  # 全部平仓

    #Else If(MarketPosition ==-1)
        print("4")
    elif data[level].A_SellPosition(0) > 0: #有空仓的情况
        if data[level].High[0] > ExitHighestPrice:
            myExitPrice = min(data[level].High[0], ExitHighestPrice + MinPoint)
            myExitPrice = data[level].Open[0] if myExitPrice > data[level].Open[0] else myExitPrice
            print("Turtle buy to cover1")
            data[level].BuyToCover(0, myExitPrice)
        else:
            if preEntryPrice!=INVALIDNUM and TurtleUnits >= 1:
                if data[level].Open[0] <= preEntryPrice - 0.5*N:
                    myEntryPrice = data[level].Open[0]
                    preEntryPrice = myEntryPrice
                    print("Turtle sell short1")
                    data[level].SellShort(TurtleUnits, myEntryPrice)

                '''while data[level].Low[0] <= preEntryPrice - 0.5*N: #以最低价为标准，判断能进行几次增仓
                    myEntryPrice = preEntryPrice - 0.5 * N
                    preEntryPrice = myEntryPrice
                    print("Turtle sell short2")
                    data[level].SellShort(TurtleUnits, myEntryPrice)'''

            if data[level].High[0] >= preEntryPrice + 2 * N: #止损指令
                myExitPrice = preEntryPrice + 2 * N
                myExitPrice = data[level].Open[0] if myExitPrice < data[level].Open[0] else myExitPrice
                print("Turtle buy to cover2")
                data[level].BuyToCover(0, myExitPrice)  # 全部平仓

        print("5")
    print("6")