ETCD_HOST="192.168.1.2"
ETCD_PORT=2379
DB_HOST="192.168.1.2"
DB_USER="admin"
DB_PASSWORD="416211"

COLLECTION_PREFIX=(
    ("bitmex","bitmex"),
)

MQ_HOST="192.168.1.2"
MQ_PORT=5672
MQ_USER="bitmex"
MQ_PASS="416211"

BIN_SIZE_LIST=(
    ("3m",("1m",3)),
    ("5m",("1m",5)),
    ("15m",("5m",15)),
    ("30m",("15m",30)),
    ("1h",("30m",60)),
    ("2h",("1h",60*2)),
    ("3h", ("1h",60*3)),
    ("4h",("2h",60*4)),
    ("6h",("3h",60*6)),
    ("8h",("4h",60*8)),
    ("12h",("6h",60*12)),
    ("1d",("12h",60*24)),
    ("2d",("1d",60*24*2)),
    ("3d",("1d",60*24*3)),
    ("4d",("1d",60*24*4)),
    ("5d",("1d",60*24*5)),
    ("1w",("1d",60*24*7)),
    ("1M",("1d",0)),
    ("1y",("1M",0)),
)
BASE_BIN_SIZE="1m"

TRADE_BUCKETED_INDICATOR_COLLECTION="trade_bucketed_indicator"

bohr_trade_bucketed_indicator_clollection="trade_bucketed_indicator"


bohr_mq={
    "host":"192.168.1.2",
    'port':5672,
    "user":"bitmex",
    "password":"416211"
}
bohr_db={
    "host":"192.168.1.2",
    "user":"admin",
    "password":"416211"
}

indicators=[{
    "key": "bitemex.com-XBTUSD-1h-ema-9-close",
    "node":"bitmex.com",
    "symbol":"XBTUSD",
    "binSize":"1h",
    "name":"ema",
    "length":9,
    "source":"close"
},{
    "key": "bitemex.com-XBTUSD-4h-ema-9-close",
    "node":"bitmex.com",
    "symbol":"XBTUSD",
    "binSize":"4h",
    "name":"ema",
    "length":9,
    "source":"close"
},{
    "key": "bitemex.com-XBTUSD-4h-macd-9-26-12-close",
    "node":"bitmex.com",
    "symbol":"XBTUSD",
    "binSize":"4h",
    "name":"macd",
    "short":9,
    "long":26,
    "signal":12,
    "source":"close"
},{
    "key": "bitemex.com-XBTUSD-4h-efi-2",
    "node":"bitmex.com",
    "symbol":"XBTUSD",
    "binSize":"4h",
    "name":"efi",
    "length":2
}]