import os
import datetime
import argparse
import pandas as pd

from . import ENGINE

datetime_format = "%Y-%m-%d %H:%M:%S"


def _get_ohlc(code, frequency, from_date=None, to_date=None, asc=True):
    """
    :param code: symbol of stock, e.g. 000001.SZ
    :param from_date: datetime alike, inclusive
    :param to_date: datetime alike, exclusive
    :param frequency: `minute` or `day`
    :return: pd.DataFrame with pricing columns and datetime index
    """
    if frequency.startswith('m'):
        table = 'stocks_minutebar'
    else:
        table = 'stocks_daybar'

    sql = f'''
      SELECT datetime, open, high, low, close, volume, amount
      FROM {table}
      WHERE code_id = {code}
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
    df.attrs['id'] = code
    return df


def _get_ohlc_adjusted(code, frequency, from_date=None, to_date=None, adjustment='forward'):
    """
    adjustment: `forward` or `backward`: not supported yet
    """
    ret = _get_ohlc(code, frequency, from_date, to_date)

    adjustments = get_adjustment(code)
    adjustments = adjustments.reindex(ret.index, method='ffill')

    # for backtrader YahooFinanceCSVData compatibility
    ret.insert(4, 'adj_close', ret['close'] * adjustments / adjustments[-1])
    return ret


def get_adjustment(code, from_date=None, to_date=None):
    if isinstance(code, str):
        sql = f"SELECT id FROM stocks_code WHERE wind_code='{code}'"
        (code,) = ENGINE.execute(sql).cursor.fetchone()

    sql = f'''
      SELECT datetime, value 
      FROM stocks_adjustment
      WHERE code_id = {code}
      AND type = 0
      ORDER BY datetime
      '''
    ret = pd.read_sql(sql, ENGINE, index_col='datetime', parse_dates=True)

    if from_date is not None:
        i = ret.index.get_loc(from_date, method='ffill')
        ret = ret.iloc[i:, ]

    if to_date is not None:
        i = ret.index.get_loc(to_date, method='ffill')
        ret = ret.iloc[:i + 1, ]

    return ret['value']


def get_pricing(code, frequency, from_date=None, to_date=None):
    sql = f"SELECT id, asset FROM stocks_code WHERE wind_code='{code}'"
    code, asset = ENGINE.execute(sql).cursor.fetchone()

    if asset == 0:  # stock
        return _get_ohlc_adjusted(code, frequency, from_date, to_date, 'forward')
    else:  # stock/bond index
        return _get_ohlc(code, frequency, from_date, to_date)


def main():
    parser = argparse.ArgumentParser(description="stock pricing to csv")

    parser.add_argument("--s", required=True, help="wind symbol for stock, stock index and bond index")
    parser.add_argument("--frequency", required=False, default='day', help="day or minute")

    args = parser.parse_args()

    df = get_pricing(args.s, args.frequency)

    # for pyalgotrade GenericCSVFeed compatibility
    df.rename(columns=str.capitalize, inplace=True)
    df.rename(columns={'Adj_close': 'Adj Close'}, inplace=True)
    df.index = df.index.strftime(datetime_format)
    df.index.name = 'Date Time'

    df.to_csv(os.path.expanduser(f'~/tmp/{args.s}_{args.frequency}.csv'))


if __name__ == "__main__":
    main()
