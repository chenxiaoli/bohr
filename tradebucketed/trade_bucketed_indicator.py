import isodate
import math
import time
import datetime
import logging
import settings
from collections import OrderedDict


class TradeBucketedIndicator(object):
    def __init__(self,collection,symbol,bin_size_list):
        self.collection=collection
        self.symbol=symbol
        self.base_bin_size=settings.BASE_BIN_SIZE
        self.bin_sizes=OrderedDict(bin_size_list)

    def create_EMA_indicator(self,length,source="close"):
        pass
    def create_MACD_indicator(self,fast,slow,source,signal):
        pass
    def create_EFI_indicator(self,length):
        pass

    def _EMA(c, N):
        Y = 0
        n = 1
        for ci in c[-N:]:
            Y = (2 * ci + (n - 1) * Y) / (n + 1)
            n += 1
        return Y