import settings

MQ_HOST = settings.MQ_HOST
MQ_PORT = settings.MQ_PORT
MQ_USER = settings.MQ_USER
MQ_PASS = settings.MQ_PASS
realtime_nodes = {"bitmex": ["XBTUSD", ]}
BIN_SIZE_LIST = ("1m","3m","5m","30m","1h","2h","4h","8h","1d")

realtime_topics = {}
for k,v in realtime_nodes.items():
    for symbol in v:
        for bin_size in BIN_SIZE_LIST:
            print("%s:%s:tradeBin%s" % (k,symbol, bin_size))
            realtime_topics["%s:%s:tradeBin%s" % (k,symbol, bin_size)] = set()
