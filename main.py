
import logging
from tradebucketed.trade_bucketed import TradeBucketed
from optparse import OptionParser
from optparse import OptionGroup
import sys

from urllib.parse import quote_plus
from pymongo import MongoClient

import settings

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
    setup_logger()
    parser = OptionParser()
    group = OptionGroup(parser, "Dangerous Options",
                        "Caution: use these options at your own risk.  "
                        "It is believed that some of them bite.")
    parser.add_option("-d", "--delete",
                     dest="delete",
                     help="remove bin size")
    options, args = parser.parse_args(sys.argv[1:])

    uri = "mongodb://%s:%s@%s" % (
        quote_plus("admin"), quote_plus("416211"), "192.168.1.2:27017")
    client = MongoClient(uri)
    collection = client["trade_data"]["bitmex_trade_bucketed"]
    symbol = "XBTUSD"
    trade_bucketed = TradeBucketed(collection, symbol, settings.BIN_SIZE_LIST)
    print(options)
    if options.delete:
        trade_bucketed.delete_many(options.delete)



