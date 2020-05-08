from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import dbfeed, membf

import re
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine

ENGINE = create_engine(
    'mysql+pymysql://rm-2zedo2m914a92z7rhfo.mysql.rds.aliyuncs.com',
    connect_args={'read_default_file': '/share/mysql.cnf'},
)


def get_ohlc(contract, from_date=None, to_date=None, freq='minute'):
    """
    :param contract: str, e.g. IF1605, A2003
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

    sql = f"SELECT id, last_traded FROM futures_contract WHERE symbol='{contract}'"
    cid, last_traded = ENGINE.execute(sql).cursor.fetchone()

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
    df.attrs['last_traded'] = last_traded
    df.attrs['contract_id'] = cid
    df.attrs['name'] = contract
    df.attrs['root_symbol'] = root_symbol
    return df


def check_date(dt0, dt1):
    dt_year, dt_week, _ = dt0.isocalendar()
    exp_year, exp_week, _ = dt1.isocalendar()
    return (dt_year, dt_week) == (exp_year, exp_week)


def check_condition(row0, row1):
    return row0['volume'] < row1['volume']


class Database(dbfeed.Database):
    def addBar(self, instrument, bar, frequency):
        raise Exception('Not supported.')

    def getBars(self, instrument, frequency, timezone=None, fromDateTime=None, toDateTime=None):
        dfs = []
        if isinstance(instrument, (list, tuple)):
            for i in instrument:
                dfs.append(get_ohlc(i, fromDateTime, toDateTime, frequency))
        else:
            dfs.append(get_ohlc(instrument, fromDateTime, toDateTime, frequency))

        if len(dfs) > 1:
            dfs = sorted(dfs, key=lambda x: x.attrs['last_traded'])

        i_freq = {'day': Frequency.DAY, 'minute': Frequency.MINUTE}[frequency]

        df = dfs.pop(0)
        df_iter = df.iterrows()
        last_traded = df.attrs['last_traded']

        ret = []
        while True:
            try:
                dt, row = next(df_iter)
            except StopIteration:
                break

            ret.append(BasicBar(
                dt,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                None,
                frequency=i_freq,
                extra={'open_interest': row['open_interest']},
            ))

            # aviod survivorship bias
            # Futures Roll Method
            # first check date then check volume
            if check_date(dt, last_traded):
                if len(dfs) and check_condition(row, dfs[0].loc[dt]):
                    df = dfs.pop(0)

                    # dt is added, so we skip it
                    idx = df.index.get_loc(dt)
                    df = df.iloc[idx + 1:]

                    df_iter = df.iterrows()
                    last_traded = df.attrs['last_traded']
                    print(f"ROLL to {df.attrs['name']} at {dt}.")

        return ret


class Feed(membf.BarFeed):
    def __init__(self, maxLen=None):
        super(Feed, self).__init__(Frequency.DAY, maxLen)
        self.__db = Database()

    def barsHaveAdjClose(self):
        return False

    def getDatabase(self):
        return self.__db

    def loadBars(self, instrument, frequency, fromDateTime=None, toDateTime=None):
        bars = self.getDatabase().getBars(
            instrument,
            frequency,
            fromDateTime=fromDateTime,
            toDateTime=toDateTime,
        )

        if isinstance(instrument, (list, tuple)):
            root_symbol = None
            for i in instrument:
                m = re.match(r'^(\w{1,2}?)\d{4}$', i)
                if m:
                    if root_symbol is None:
                        root_symbol = m.group(1)
                    else:
                        assert root_symbol == m.group(
                            1), 'Multiple futures are not supported.'
                else:
                    raise Exception('Not supported contract.')
        else:
            root_symbol = instrument

        self.addBarsFromSequence(root_symbol, bars)
