import pandas as pd
from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import dbfeed, membf
from secdb.reader import get_ohlcv


class Database(dbfeed.Database):
    def _get_ohlcv(self, instrument, fromDateTime=None, toDateTime=None):
        return get_ohlcv(instrument, fromDateTime, toDateTime, adj=True)

    def getBars(self, instrument, fromDateTime=None, toDateTime=None, benchmark=None):
        data = self._get_ohlcv(instrument, fromDateTime, toDateTime)

        # If we want to use a cumulative active returns series
        # we only care about adjusted closing price
        if benchmark is not None:
            benchmark = self._get_ohlcv(benchmark, fromDateTime, toDateTime)
            common_dates = data.index.union(benchmark.index)

            data = data[['adj_close']].reindex(index=common_dates, method='ffill').pct_change().fillna(0)
            benchmark = benchmark[['adj_close']].reindex(index=common_dates, method='ffill').pct_change().fillna(0)

            data = (data - benchmark + 1).cumprod()

            data['open'] = data['adj_close']
            data['high'] = data['adj_close']
            data['low'] = data['adj_close']
            data['close'] = data['adj_close']
            data['volume'] = 1000000

        ret = []
        for dateTime, row in data.iterrows():
            ret.append(BasicBar(
                dateTime,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['adj_close'],
                'daily',
            ))
        return ret


class Feed(membf.BarFeed):
    def __init__(self, maxLen=None):
        super(Feed, self).__init__(Frequency.DAY, maxLen)
        self.__db = Database()

    def barsHaveAdjClose(self):
        return True

    def getDatabase(self):
        return self.__db

    def loadBars(self, instrument, fromDateTime=None, toDateTime=None, benchmark=None):
        bars = self.getDatabase().getBars(instrument, fromDateTime, toDateTime, benchmark)
        self.addBarsFromSequence(instrument, bars)
