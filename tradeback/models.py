from mongoengine import *


# class OpNode(EmbeddedDocument):
#     version = StringField("运算版本", required=True)
#     parent = EmbeddedDocument('OpNode', null=True, blank=True)
#     left = EmbeddedDocument('OpNode', null=True, blank=True)
#     right = EmbeddedDocument('OpNode', null=True, blank=True)
#     operator = StringField("运算操作")
#     variables = ListField()


class DataSet(EmbeddedDocument):
    node = StringField(required=True)
    symbol = StringField(required=True)
    bin_size = StringField(required=True)

    def __str__(self):
        return str({"node": self.node, "symbol": self.symbol, "binSize": self.bin_size})

    def to_dict(self):
        return {"node": self.node, "symbol": self.symbol, "binSize": self.bin_size}


class Factor(EmbeddedDocument):
    """
    expression example:
        [("cross", ("macd", (13, 30, 9, "close"), [-2, -1]), 0),
                                    ("up", ("macd", (13, 30, 9, "close"), [-2, -1])),
                                    ("cross", ("ema", (8, "close"),[-2,-1]), ("ema", (26, "close"),[-2,-1]))]

    """
    operator_version = StringField(required=True)  # "运算操作版本号",
    data_set = EmbeddedDocumentField(DataSet)
    expressions = ListField(required=True)  # "表达式",

    def __str__(self):
        return str(
            {"operator_version": self.operator_version, "dataSet": self.data_set, "expressions": self.expressions})

    def get_indicators(self):
        _expressions = self.expressions
        _indicators = []
        for _exp in _expressions:
            for _c in _exp:
                if type(_c) is tuple and _c[0] in ("macd", "ema", "efi"):
                    _indicator = _c[0]
                    _args = _c[1]
                    _indicators.append({"dataSet": self.data_set, "indicator": (_indicator, _args)})
        return _indicators


class Strategy(Document):
    """"
    position_rate:每次建仓占总资产比例。
    """
    name = StringField(required=True)  # 策略名称",
    start_money = FloatField(required=True, default=100)  # "初始金额",
    buy_trend_factors = EmbeddedDocumentListField(Factor)  # 做多趋势确认因子
    buy_creator_factors = EmbeddedDocumentListField(Factor)  # 卖出操作因子
    sell_trend_factors = EmbeddedDocumentListField(Factor)  # 看空趋势确认因子
    sell_creator_factors = EmbeddedDocumentListField(Factor)  # 卖出操作因子
    stop_loss_factors = EmbeddedDocumentListField(Factor)  # 止损策略
    stop_profit_factors = EmbeddedDocumentListField(Factor)  # 止盈策略
    position_rate = FloatField()  # "仓位比例"


class PositionStrategy(Document):
    """买入/卖出 趋势确认"""
    strategy = ReferenceField(Strategy)
    node = StringField(required=True)
    symbol = StringField(required=True)
    bin_size = StringField(required=True)
    timestamp = DateTimeField(required=True)
    action = StringField(required=True)  # buy, sell


class PositionCreator(Document):
    """买入/卖出"""
    strategy = ReferenceField(Strategy)
    node = StringField(required=True)
    symbol = StringField(required=True)
    bin_size = StringField(required=True)
    timestamp = DateTimeField(required=True)
    action = StringField(required=True, choices=())  # buy, sell
    price = FloatField(required=True)
    stop_loss_price = FloatField(required=True)
    stop_win_price = FloatField(required=True)


class Order(Document):
    """策略回测下单记录表"""
    strategy = ReferenceField(Strategy)
    timestamp = DateTimeField()
    position = FloatField(required=True, default=0)
    money = FloatField(required=True, default=0)
    position_strategy = ReferenceField(PositionStrategy)
    position_creator = ReferenceField(PositionCreator)


# class Balance(Document):
#     account = ReferenceField(Account)
#     strategy = ReferenceField(Strategy)
#     symbol = StringField()
#     timestamp = DateTimeField()
