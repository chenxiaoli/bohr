import logging
from urllib.parse import quote_plus
from time import sleep
from pymongo import MongoClient

import settings

from tradebucketed.trade_bucketed import TradeBucketed

# Basic use of websocket.
def run():
    logger = setup_logger()

    # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.

    uri = "mongodb://%s:%s@%s" % (
        quote_plus("admin"), quote_plus("416211"), "192.168.1.2:27017")
    client = MongoClient(uri)
    collection=client["trade_data"]["bitmex_trade_bucketed"]
    symbol="XBTUSD"
    base_bin_size="1m"
    trade_bucketed=TradeBucketed(collection,symbol,base_bin_size,settings.BIN_SIZE_LIST)
    # Run forever
    while True:
        trade_bucketed.increase_create_trade_bucketed()



def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    run()
    # import datetime
    # last_one_time=datetime.datetime(2001,1,1)
    # isoweekday=last_one_time.isoweekday()
    # last_one_time=last_one_time-datetime.timedelta(days=isoweekday-1)
    # print(isoweekday,last_one_time.isoweekday())

