import os
import re
import datetime
import pandas as pd
from more_itertools import pairwise
from sqlalchemy import create_engine
from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import membf

OHLC = ['open', 'high', 'low', 'close']
ENGINE = create_engine(
    'mysql+pymysql://rm-2zedo2m914a92z7rhfo.mysql.rds.aliyuncs.com',
    connect_args={'read_default_file': os.path.expanduser('~/my.cnf')},
)

DEFAULT_VERSION = 1


def get_cf(root_symbol, version=DEFAULT_VERSION):
    sql = f'''
      SELECT datetime, contract_id 
      FROM futures_continuousfutures
      WHERE root_symbol_id = (
        SELECT id FROM futures_rootsymbol
        WHERE symbol='{root_symbol}'
      )
      AND version = {version}
      '''
    df = pd.read_sql(sql, ENGINE, parse_dates=['datetime'])
    return df


def get_ohlc(contract, freq='minute', from_date=None, to_date=None, asc=True):
    """
    :param contract: symbol of contract str, e.g. IF1605, A2003
    :param from_date: datetime alike, inclusive
    :param to_date: datetime alike, exclusive
    :param freq: `minute` or `day`
    :return: pd.DataFrame with pricing columns and datetime index
    """
    if freq.startswith('m'):
        table = 'futures_minutebar'
    else:
        table = 'futures_dailybar'

    if isinstance(contract, str):
        sql = f"SELECT id FROM futures_contract WHERE symbol='{contract}'"
        (contract,) = ENGINE.execute(sql).cursor.fetchone()

    sql = f'''
      SELECT datetime, open, high, low, close, volume, open_interest
      FROM {table}
      WHERE contract_id = {contract}
      '''

    if from_date is not None:
        from_date = pd.Timestamp(from_date)
        sql += f" AND datetime >= '{from_date}'"

    if to_date is not None:
        to_date = pd.Timestamp(to_date)
        sql += f" AND datetime < '{to_date}'"

    sql += ' ORDER BY datetime'
    if not asc:
        sql += ' DESC'

    df = pd.read_sql(sql, ENGINE, parse_dates=True, index_col=['datetime'])
    return df


def get_ohlc_cf(cf, frequency, from_date=None, to_date=None):
    """
    :param cf: list of tuple, e.g.
        [('IF1906', datetime.datetime(2019, 5, 16)),
         ('IF1907', datetime.datetime(2019, 6, 21)),
         ('IF1908', datetime.datetime(2019, 7, 19)),]
        or
        [(637, datetime.datetime(2019, 5, 16)),
         (638, datetime.datetime(2019, 6, 21)),
         (639, datetime.datetime(2019, 7, 19)),]
    :param from_date: datetime alike, inclusive
    :param to_date: datetime alike, exclusive
    :param frequency: `minute` or `day`
    :return: pd.DataFrame with pricing columns and datetime index
    """
    # adjust cf by from_date and to_date
    cf.sort(key=lambda x: x[1])

    if from_date and isinstance(from_date, str):
        from_date = pd.Timestamp(from_date)

    if to_date and isinstance(to_date, str):
        to_date = pd.Timestamp(to_date)

    i, j = 0, 0
    for c, t1 in cf:
        if from_date and t1 <= from_date:
            i += 1
        if to_date and t1 >= to_date:
            j += 1

    for _ in range(1, i):
        cf.pop(0)  # skip the contract before
    for _ in range(j - 1):
        cf.pop()  # skip the contract after

    ohlcs = []
    ohlc = None
    for (c0, t0), (c1, t1) in pairwise(cf + [(None, None)]):
        if c1 is None:
            end = to_date
            if ohlc is not None:
                start = ohlc.index[0]
            else:
                start = t0 if from_date is None else max(from_date, t0)
        else:
            end = t1
            if ohlc is not None:
                start = ohlc.index[0]
            else:
                start = t0 if from_date is None else max(from_date, t0)

        ohlc = get_ohlc(c0, frequency, start, end, asc=False)
        ohlcs.append(ohlc)

    ret = None
    for ohlc in ohlcs[::-1]:
        if ret is None:
            ret = ohlc
            continue

        t_adjustment = ohlc.index[0]
        assert t_adjustment == ret.index[-1]

        raw = ohlc.loc[t_adjustment, 'close']  # always adjusted
        adj = ret.loc[t_adjustment, 'close']

        ohlc[OHLC] += adj - raw
        ret = pd.concat([ret.iloc[:-1], ohlc])

    return ret.sort_index()


def df_to_list_of_bar(df, frequency, mul=1):
    ret = []
    for dt, row in df.iterrows():
        ret.append(BasicBar(
            dt,
            row['open'] * mul,
            row['high'] * mul,
            row['low'] * mul,
            row['close'] * mul,
            row['volume'],
            None,
            frequency=frequency,
            extra={'open_interest': row['open_interest']},
        ))
    return ret


class BarFeed(membf.BarFeed):
    def __init__(self, frequency, maxLen=None):
        assert frequency in (Frequency.MINUTE, Frequency.DAY), \
            'Only Frequency.DAY or Frequency.MINUTE are supported.'
        super(BarFeed, self).__init__(frequency, maxLen)

    def barsHaveAdjClose(self):
        return False

    def loadBars(self, instrument, from_date=None, to_date=None, multiplier=1):
        frequency = {Frequency.DAY: 'day', Frequency.MINUTE: 'minute'}[self.getFrequency()]

        m = re.match(r'^(\w{1,2}?)\d{4}$', instrument)
        if m:  # single contract
            df = get_ohlc(instrument, frequency, from_date, to_date)
        else:  # continuous futures
            cf = get_cf(instrument)
            cf = list(zip(cf['contract_id'], cf['datetime']))
            df = get_ohlc_cf(cf, frequency, from_date, to_date)

        bars = df_to_list_of_bar(df, self.getFrequency(), multiplier)
        self.addBarsFromSequence(instrument, bars)
