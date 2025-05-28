import abc
class DBApi(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def insert_tradaccount(self, pTradingAccount):
        pass

    @abc.abstractmethod
    def update_tradaccount(self, pTradingAccount):
        pass

    @abc.abstractmethod
    def insert_orderinfo(self, pOrderInfo):
        pass

    @abc.abstractmethod
    def update_orderinfo(self, pOrderInfo):
        pass

    @abc.abstractmethod
    def insert_tradeinfo(self, pTrade):
        pass

    @abc.abstractmethod
    def insert_investor_position(self, pInvestorPosition):
        pass

    @abc.abstractmethod
    def update_investor_position(self, pInvestorPosition):
        pass


