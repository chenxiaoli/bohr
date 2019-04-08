from mongoengine import *

class TradeBucketedIndicatorEntity(Document):
    node= StringField(max_length=20, required=True)#交易所，如bitmex.com,hbg.com,okex.com
    symbol = StringField(max_length=20, required=True)
    bin_size = StringField( required=True,db_field="binSize")
    timestamp=DateTimeField( required=True)
    meta = {'allow_inheritance': True,
            'indexes': [[("node", 1), ("symbol", 1), ("bin_size", 1), ("timestamp", 1)]],
            'collection': 'trade_bucketed_indicator',
            'abstract': True,
            }


class EmaEmbed(EmbeddedDocument):
    source=StringField(max_length=20,required=True)
    length=IntField(required=True)
    price=FloatField()

class EmaEntity(TradeBucketedIndicatorEntity):
    ema=ListField(EmbeddedDocumentField(EmaEmbed))


class MacdEmbed(EmbeddedDocument):
    source=StringField(max_length=20,required=True)
    short=IntField(required=True)
    long = IntField(required=True)
    signal = IntField(required=True)
    # short_ema=FloatField(required=True)
    # long_ema = FloatField(required=True)
    diff=FloatField(required=True)
    dea=FloatField(required=True)

class MacdEntity(TradeBucketedIndicatorEntity):
    macd=ListField(EmbeddedDocumentField(MacdEmbed))
    ema=ListField(EmbeddedDocumentField(EmaEmbed))
    def get_macd(self,source,short,long,signal):
        pass



