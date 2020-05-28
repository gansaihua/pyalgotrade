"""
Backward adjustment for rollover
"""

import os
import re
import logging
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine
from pyalgotrade import logger
from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import membf

# days before last traded of contract to roll by calendar
DEFAULT_GRACE_DAYS = 5

ENGINE = create_engine(
    'mysql+pymysql://rm-2zedo2m914a92z7rhfo.mysql.rds.aliyuncs.com',
    connect_args={'read_default_file': os.path.expanduser('~/my.cnf')},
)

log = logger.getLogger(__name__)


def get_contracts(root_symbol, from_date=None, to_date=None, included=None):
    """
    :param root_symbol: root symbol of contracts, e.g. IF, A
    :param from_date: datetime alike
    :param to_date: datetime alike
    :param freq: `minute` or `day`
    :param inclued: None or list e.g. [6], [3, 6, 9, 12], contracts of specific months to include
    :return: list of contract symbols
    """

    sql = f'''
    SELECT c.symbol
    FROM futures_contract AS c
    JOIN futures_rootsymbol AS rs
    ON c.root_symbol_id = rs.id
    WHERE rs.symbol = '{root_symbol}' 
    '''

    if included is not None:
        if len(included) == 1:
            sql += f' AND RIGHT(c.symbol, 2) = {int(included[0]):02}'
        else:
            included = (f'{int(x):02}' for x in included)
            sql += f' AND RIGHT(c.symbol, 2) IN {tuple(included)}'

    if from_date is not None:
        from_date = pd.Timestamp(from_date)
        sql += f" AND last_traded >= '{from_date}'"

    if to_date is not None:
        to_date = pd.Timestamp(to_date)
        sql += f" AND contract_issued <= '{to_date}'"

    sql += ' ORDER BY last_traded'

    symbols = ENGINE.execute(sql).cursor.fetchall()
    return [symbol[0] for symbol in symbols]


def get_ohlc(contract, from_date=None, to_date=None, freq='minute'):
    """
    :param contract: symbol of contract str, e.g. IF1605, A2003
    :param from_date: datetime alike
    :param to_date: datetime alike
    :param freq: `minute` or `day`
    :return: pd.DataFrame with pricing columns and datetime index, and metadata in attrs
    """
    if freq.startswith('m'):
        table = 'futures_minutebar'
    else:
        table = 'futures_dailybar'

    m = re.match(r'^(\w{1,2}?)\d{4}$', contract)
    if m:
        root_symbol = m.group(1)
    else:
        raise Exception('Not supported contract.')

    sql = f'''
        SELECT id, tick_size, multiplier, last_traded 
        FROM futures_contract
        WHERE symbol='{contract}'
        '''
    cid, tick_size, multiplier, last_traded = ENGINE.execute(sql).cursor.fetchone()

    sql = f'''
      SELECT datetime, open, high, low, close, volume, open_interest
      FROM {table}
      WHERE contract_id = {int(cid)}
      '''

    if from_date is not None:
        from_date = pd.Timestamp(from_date)
        sql += f" AND datetime >= '{from_date}'"

    if to_date is not None:
        to_date = pd.Timestamp(to_date)
        sql += f" AND datetime <= '{to_date}'"

    sql += ' ORDER BY datetime'

    df = pd.read_sql(sql, ENGINE, parse_dates=True, index_col=['datetime'])
    df.attrs['name'] = contract
    df.attrs['root_symbol'] = root_symbol
    df.attrs['multiplier'] = multiplier
    df.attrs['tick_size'] = tick_size
    df.attrs['last_traded'] = last_traded
    return df


def check_date(dt0, dt1, grace_days=DEFAULT_GRACE_DAYS):
    return dt0 >= dt1 - timedelta(days=grace_days)


def check_condition(row0, row1):
    return row0['open_interest'] <= row1['open_interest'] or \
           row0['volume'] <= row1['volume']


def getBars(instruments, frequency, from_date=None, to_date=None, grace_days=DEFAULT_GRACE_DAYS):
    s_frequency = {Frequency.DAY: 'day', Frequency.MINUTE: 'minute'}[frequency]

    dfs = [get_ohlc(instrument, from_date, to_date, s_frequency)
           for instrument in instruments]

    if len(dfs) > 1:
        dfs = sorted(dfs, key=lambda x: x.attrs['last_traded'])

    df = dfs.pop(0)
    adjustment = 0

    logger.Formatter.DATETIME_HOOK = lambda: df.index[0]
    log.debug(f"{df.attrs['name']}: {adjustment}")

    ret = []
    df_iter = df.iterrows()
    while True:
        try:
            dt, row = next(df_iter)
        except StopIteration:
            break

        # we skip the day without trades
        if row['volume'] > 0 and row['open'] is not None and not np.isnan(row['open']):
            mul = df.attrs['multiplier']
            ret.append(BasicBar(
                dt,
                (row['open'] + adjustment) * mul,
                (row['high'] + adjustment) * mul,
                (row['low'] + adjustment) * mul,
                (row['close'] + adjustment) * mul,
                row['volume'],
                None,
                frequency=frequency,
                extra={**df.attrs, 'open_interest': row['open_interest']},
            ))

        # Futures Roll Method
        # roll by open interest and volume
        # to avoid lookahead bias, we should roll after adding bar
        if dfs and check_date(dt, df.attrs['last_traded'], grace_days):
            if dt in dfs[0].index and check_condition(row, dfs[0].loc[dt]):
                df = dfs.pop(0)

                # forward adjustment factor
                adjustment += row['close'] - df.loc[dt, 'close']

                # dt is added, so we skip it
                idx = df.index.get_loc(dt)
                df = df.iloc[idx + 1:]

                df_iter = df.iterrows()

                logger.Formatter.DATETIME_HOOK = lambda: dt
                log.debug(f"{df.attrs['name']}: {adjustment}")

        # reset the global formatter
        logger.Formatter.DATETIME_HOOK = None
    return ret


class BarFeed(membf.BarFeed):
    def __init__(self, frequency, maxLen=None):
        assert frequency in (Frequency.MINUTE, Frequency.DAY), 'Only 86400 and 60 are supported.'
        super(BarFeed, self).__init__(frequency, maxLen)

    def barsHaveAdjClose(self):
        return False

    def setDebugMode(self, debugOn):
        """
        Debug and print roll dates and forward adjustment factors
        """
        level = logging.DEBUG if debugOn else logging.INFO
        log.setLevel(level)

    def loadBars(self,
                 instrument,
                 from_date=None,
                 to_date=None,
                 included=None,
                 grace_days=DEFAULT_GRACE_DAYS):

        if isinstance(instrument, str):
            root_symbol = instrument
            contracts = get_contracts(instrument, from_date, to_date, included)
            if not contracts:  # single contract
                contracts = [instrument]
        elif isinstance(instrument, (list, tuple)):  # list of contracts
            root_symbol = None
            contracts = instrument
            for contract in contracts:
                m = re.match(r'^(\w{1,2}?)\d{4}$', contract)
                if m:
                    if root_symbol is None:
                        root_symbol = m.group(1)
                    else:
                        assert root_symbol == m.group(1), 'Multiple futures are not supported.'
                else:
                    raise Exception('Not supported contract.')
        else:
            raise Exception('Not supported instrument.')

        bars = getBars(contracts, self.getFrequency(), from_date, to_date, grace_days)
        self.addBarsFromSequence(root_symbol, bars)
