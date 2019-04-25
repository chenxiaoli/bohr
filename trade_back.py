import logging
from urllib.parse import quote_plus
from pymongo import MongoClient
import settings
from tradeback.strategy import Strategy,TripleShortTrade
from tradeback.strategy import DataSource
from tradebucketed.trade_bucketed_indicator import IndicatorSummary


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

    buy_strategy_factors=[{
            "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"30m",
                },
            "apply":[("cross",("macd",(13,30,9,"close"),[-2,-1]),0),("up",("macd",(13,30,9,"close"),[-2,-1])),("up",("ema",(8,"close"),[-2,-1]))]
          },]
    buy_action_factors=[{
        "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"3m",
                },
        "apply":[("up",("efi",(2,),[-2,-1])),("cross",("efi",(2,),[-1,-2]),0)],
    },]

    uri = "mongodb://%s:%s@%s" % (
        quote_plus(settings.DB_USER), quote_plus(settings.DB_PASSWORD), settings.DB_HOST)

    uri2 = "mongodb://%s/%s" % (settings.DB_HOST, "trade_data")
    client = MongoClient(uri)
    db = client["trade_data"]
    collection=db["trade_bucketed_indicator"]
    buy_strategy=Strategy(factors=buy_strategy_factors,collection=collection)
    buy=Strategy(factors=buy_action_factors,collection=collection)

    money=100
    max_loss=0

    sell_strategy_factors=[{
            "dataSet":{
                "node":"bitmex.com",
                "symbol":"XBTUSD",
                "binSize":"30m",
                },
            "apply":[("cross",("macd",(12,30,9,"close"),[-2,-1]),0),("down",("macd",12,30,9,"close"),[-2,-1]),("up",("ema",(8,"close"),[-2,-1]))]
          },]


    sell_action_factors = [{"method":"cross",
            "params":{"indicator": "efi", "params": {"binSize": "3m", "length": 2}},
            },
           {"method": "up",
            "params": {"indicator": "efi", "params": {"binSize": "3m", "length": 2}},
            }
           ]
    sell_strategy = Strategy(factors=sell_strategy_factors, collection=collection)
    sell=Strategy(factors=sell_action_factors,collection=collection)

    trade_bucketed_collection = db["bitmex_trade_bucketed"]

    # triple=TripleShortTrade(trade_bucketed_collection,buy_strategy,buy,sell_strategy,sell,money,max_loss)
    # triple.forward()
    # buy_strategy.apply()


    for factors in buy_action_factors:
        _data_set=factors["dataSet"]
        _exps=factors["apply"]
        for _exp in _exps:
            for _s in _exp:
                if type(_s) is tuple and _s[0] in ("macd","ema","efi"):
                    _summary = IndicatorSummary(db=db, data_set=_data_set, data=_s)
                    _summary.add_or_update()
