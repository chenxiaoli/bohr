"""
交易策略：
1.创建买入信号策略
2.创建买入
2.创建卖出策略
3.初始金额
4..
5.买入价位
6.盈利目标价
7.止损价位

投资组合绩效表
1.净利润
2.交易规模
3.交易天数
4.最大回调
5.最大跌幅的持续时间
6.最大连续亏损
7.盈利与最大亏损比
8.盈亏比例
9.胜率
10.时间百分比


1.买入信号列表

"""
from mongoengine import connect
import logging
from urllib.parse import quote_plus
from pymongo import MongoClient
import settings
from tradeback.strategy import Strategy, BackTester
from tradeback.models import Strategy as StrategyEntity, DataSet, Factor
from tradebucketed.trade_bucketed_indicator import IndicatorSummary

uri = "mongodb://%s:%s@%s" % (
    quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)

uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")
client = MongoClient(uri)
db = client["trade_data"]
collection = db["trade_bucketed_indicator"]
connect(db='trade_data',username=settings.DB_USER,
        password=settings.DB_PASSWORD,authentication_source='admin')


def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == '__main__':
    setup_logger()

    bin_size_30m = DataSet(node="bitmex.com", symbol="XBTUSD", bin_size="30m")
    bin_size_3m = DataSet(node="bitmex.com", symbol="XBTUSD", bin_size="3m")

    buy_trend_factors = []
    buy_trend_factor = Factor(operator_version="1.0", data_set=bin_size_30m)
    buy_trend_factor.expressions = [("cross", ("macd", (12, 26, 9, "close"), [-2, -1]), 0),
                                    ("up", ("macd", (13, 30, 9, "close"), [-2, -1])),
                                    ("up", ("ema", (8, "close"), [-2, -1]))]

    buy_trend_factors.append(buy_trend_factor)

    buy_creator_factor = Factor(operator_version="1.0", data_set=bin_size_3m)
    buy_creator_factor.expressions = [("enterPrice", ("market", 0)), ]
    buy_creator_factors = [].append(buy_creator_factor)

    sell_trend_factors = []
    sell_trend_factor = Factor(operator_version="1.0", data_set=bin_size_30m)
    sell_trend_factor.expressions = [("cross", ("macd", (12, 30, 9, "close"), [-2, -1]), 0),
                                     ("down", ("macd", (12, 30, 9, "close"), [-2, -1])),
                                     ("up", ("ema", (5, "close"), [-2, -1]))]
    sell_trend_factors.append(sell_trend_factor)

    sell_creator_factors = []
    sell_creator_factor = Factor(operator_version="1.0", data_set=bin_size_3m)
    sell_creator_factor.expressions = [("cross", ("efi", (2,), [-2, -1]), 0), ]
    sell_creator_factors.append(sell_creator_factor)

    stop_loss_factors = []
    stop_loss_factor = Factor(operator_version="1.0", data_set=bin_size_3m)
    stop_loss_factor.expressions = [("loss", 0.1)]
    stop_loss_factors.append(stop_loss_factor)

    stop_profit_factors = []
    stop_profit_factor = Factor(operator_version="1.0", data_set=bin_size_30m)
    stop_profit_factor.expressions = [("profit", 0.2)]
    stop_profit_factors.append(stop_profit_factor)

    strategy_entity = StrategyEntity(name="backtester1", start_money=100, position_rate=0.1)
    strategy_entity.buy_trend_factors = buy_trend_factors
    strategy_entity.buy_creator_factors = buy_creator_factors
    strategy_entity.sell_trend_factors = sell_trend_factors
    strategy_entity.sell_creator_factors = sell_creator_factors
    strategy_entity.stop_loss_factors = stop_loss_factors
    strategy_entity.stop_profit_factors = stop_profit_factors

    logging.debug("buy_trend_factors:%s" % buy_trend_factors)
    logging.debug("stop_loss_factors:%s" % stop_loss_factors)

    # for factors in buy_trend_factor.expressions:
    #     _data_set = factors["dataSet"]
    #     _exps = factors["apply"]
    #     for _exp in _exps:
    #         for _s in _exp:
    #             if type(_s) is tuple and _s[0] in ("macd", "ema", "efi"):
    #                 _summary = IndicatorSummary(db=db, data_set=_data_set, data=_s)
    #                 _summary.add_or_update()

    strategy_entity.save()
    backteseter = BackTester(strategy_entity)
    for data_set_indicator in backteseter.data_set:
        data_set = data_set_indicator["dataSet"]
        indicator = data_set_indicator["indicator"]
        indicator_summary = IndicatorSummary(db=db,data_set=data_set.to_dict(), data=indicator)
        indicator_summary.add_or_update()
    logging.debug(backteseter.data_set)
    backteseter.forward()
    # backteseter.apply()

