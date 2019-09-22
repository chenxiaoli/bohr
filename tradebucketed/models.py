from mongoengine import *


class TradeBucketed(Document):
    symbol = StringField(required=True)
    bin_size = StringField(required=True)
    open = FloatField()
    close = FloatField()
    high = FloatField()
    volume = FloatField()
    timestamp = DateTimeField()


class BaseTradeBucketedIndicator(Document):
    name = StringField(required=True, choices=("macd", "efi", "ema"))
    key = StringField(required=True, unique=True)
    node = StringField(required=True)
    symbol = StringField(required=True)
    bin_size = StringField(required=True)
    meta = {
        "abstract": True,
    }


class MacdIndicator(BaseTradeBucketedIndicator):
    short = IntField(required=True)
    long = IntField(required=True)
    signal = IntField(required=True)
    source = StringField(required=True)


class EmaIndicator(BaseTradeBucketedIndicator):
    length = IntField(required=True)
    source = StringField(required=True)


class EfiIndicator(BaseTradeBucketedIndicator):
    length = IntField(required=True)
