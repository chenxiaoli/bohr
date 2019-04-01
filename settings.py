DB_HOST="192.168.1.2"
DB_USER="admin"
DB_PASSWORD="416211"

COLLECTION_PREFIX=(
    ("bitmex","bitmex"),
)

BIN_SIZE_LIST=(
    ("3m",("1m",3)),
    ("5m",("1m",5)),
    ("15m",("1m",15)),
    ("30m",("1m",30)),
    ("1h",("1m",60)),
    ("2h",("1m",60*2)),
    ("3h", ("1m", 60*3)),
    ("4h",("1m",60*4)),
    ("6h",("1m",60*6)),
    ("8h",("1m",60*8)),
    ("12h",("1m",60*12)),
    ("1d",("1m",60*24)),
    ("2d",("1d",60*24*2)),
    ("4d",("2d",60*24*4)),
    ("1w",("1d",60*24*7)),
    ("1M",("1d",0)),
    ("1y",("1M",0)),
)
BASE_BIN_SIZE="1m"

TRADE_BUCKETED_INDICATOR_COLLECTION="trade_bucketed_indicator"
