import re
import pandas as pd
from sqlalchemy import create_engine
from pyalgotrade.bar import BasicBar, Frequency
from pyalgotrade.barfeed import dbfeed, membf

ENGINE = create_engine(
    'mysql+pymysql://rm-2zedo2m914a92z7rhfo.mysql.rds.aliyuncs.com',
    connect_args={'read_default_file': '/share/mysql.cnf'},
)


def get_contracts(root_symbol, from_date=None, to_date=None):
    """
    :param root_symbol: root symbol of contracts, e.g. IF, A
    :param from_date: datetime alike
    :param to_date: datetime alike
    :param freq: `minute` or `day`
    :return: list of contract symbols
    """

    sql = f'''
    SELECT c.symbol
    FROM futures_contract AS c
    JOIN futures_rootsymbol AS rs
    ON c.root_symbol_id = rs.id
    WHERE rs.symbol = '{root_symbol}' 
    '''

    if from_date is not None:
        from_date = pd.Timestamp(from_date)
        sql += f" AND last_traded >= '{from_date}'"

    if to_date is not None:
        to_date = pd.Timestamp(to_date)
        sql += f" AND last_traded <= '{to_date}'"

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


def check_condition(row0, row1, use_oi=True):
    ret = row0['volume'] <= row1['volume']
    if use_oi:
        ret &= row0['open_interest'] <= row1['open_interest']
    return ret


class Database(dbfeed.Database):
    def addBar(self, instrument, bar, frequency):
        raise Exception('Not supported.')

    def getBars(self, instrument, frequency, timezone=None, fromDateTime=None, toDateTime=None):
        dfs = []
        if isinstance(instrument, (list, tuple)):
            for i in instrument:
                df = get_ohlc(i, fromDateTime, toDateTime, frequency)
                if not df.empty:
                    dfs.append(df)
        elif isinstance(instrument, str):
            df = get_ohlc(instrument, fromDateTime, toDateTime, frequency)
            if not df.empty:
                dfs.append(df)
        else:
            raise Exception('Not supported instrument.')

        if len(dfs) > 1:
            dfs = sorted(dfs, key=lambda x: x.attrs['last_traded'])

        i_freq = {'day': Frequency.DAY, 'minute': Frequency.MINUTE}[frequency]

        df = dfs.pop(0)
        df_iter = df.iterrows()
        last_traded = df.attrs['last_traded']

        # LOGGING: enter the first contract
        print(f"ROLL to {df.attrs['name']} at {df.index[0]}.")

        ret = []
        while True:
            try:
                dt, row = next(df_iter)
            except StopIteration:
                break

            # To aviod lookahead bias
            # place this block before the contract roll checking
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

            # Futures Roll Method
            # first check date then check volume and open interest
            if check_date(dt, last_traded):
                if len(dfs) and check_condition(row, dfs[0].loc[dt], True):
                    df = dfs.pop(0)

                    # dt is added, so we skip it
                    idx = df.index.get_loc(dt)
                    df = df.iloc[idx + 1:]

                    df_iter = df.iterrows()
                    last_traded = df.attrs['last_traded']

                    # LOGGING: enter the successive contract
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
        if isinstance(instrument, str):
            root_symbol = instrument
            contracts = get_contracts(instrument, fromDateTime, toDateTime)
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

        bars = self.getDatabase().getBars(
            contracts,
            frequency,
            fromDateTime=fromDateTime,
            toDateTime=toDateTime,
        )
        self.addBarsFromSequence(root_symbol, bars)
