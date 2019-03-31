from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import dbfeed, membf
from secdata.reader import (
    get_sid,
    get_asset_class,
    get_pricing,
)
from secdata.utils import fill_ohlcv


class Database(dbfeed.Database):
    def get_ohlcv(self, instrument, fromDateTime=None, toDateTime=None):
        if isinstance(instrument, str):
            instrument = get_sid(instrument)

        data = get_pricing(instrument, fromDateTime, toDateTime)

        asset_class = get_asset_class(instrument)
        if asset_class == 'stock':
            data['adj_close'] = data['close'] * data['adjfactor'] / data['adjfactor'].iloc[-1]
        else:
            data = fill_ohlcv(data)
            data['adj_close'] = data['close']
        return data


    def getBars(self, instrument, fromDateTime=None, toDateTime=None, benchmark=None):
        data = self.get_ohlcv(instrument, fromDateTime, toDateTime)

        # If we want to use a cumulative excess returns series
        # we only care about adjusted closing price
        if benchmark is not None:
            benchmark = self.get_ohlcv(benchmark, fromDateTime, toDateTime)
            common_dates = data.index.join(benchmark.index)

            data = data[['adj_close']].reindex(index=common_dates).pct_change().iloc[1:]
            benchmark = benchmark[['adj_close']].reindex(index=common_dates).pct_change().iloc[1:]

            data = (data - benchmark + 1).cumprod()

            data['open'] = data['adj_close']
            data['high'] = data['adj_close']
            data['low'] = data['adj_close']
            data['close'] = data['adj_close']
            data['volume'] = 10000

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
