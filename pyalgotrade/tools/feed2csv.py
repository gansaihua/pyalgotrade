import os
import csv
import argparse
from pyalgotrade import dispatcher
from pyalgotrade.barfeed.futuresfeed import Feed

TRADED_INSTRUMENTS = {
    # DCE
    'C': {'included': [1, 5, 9], 'grace_days': 200},
    'CS': {'included': [1, 5, 9], 'grace_days': 200},
    'A': {'included': [1, 5, 9], 'grace_days': 200},
    'B': {'included': [1, 5, 9], 'grace_days': 200},
    'M': {'included': [1, 5, 9], 'grace_days': 200},
    'Y': {'included': [1, 5, 9], 'grace_days': 200},
    'P': {'included': [1, 5, 9], 'grace_days': 200},
    'JD': {'included': [1, 5, 9], 'grace_days': 200},
    'L': {'included': [1, 5, 9], 'grace_days': 200},
    'I': {'included': [1, 5, 9], 'grace_days': 200},
    'J': {'included': [1, 5, 9], 'grace_days': 200},
    'JM': {'included': [1, 5, 9], 'grace_days': 200},
    'EG': {'included': [1, 5, 9], 'grace_days': 200},
    'V': {'included': [1, 5, 9], 'grace_days': 200},
    'PP': {'included': [1, 5, 9], 'grace_days': 200},
    # SHF
    'CU': {'grace_days': 60},
    'AL': {'grace_days': 60},
    'ZN': {'grace_days': 60},
    'PB': {'grace_days': 60},
    'NI': {'grace_days': 60},
    'SN': {'grace_days': 60},
    'AU': {'included': [6, 12], 'grace_days': 200},
    'AG': {'included': [6, 12], 'grace_days': 200},
    'BU': {'included': [6, 9, 12], 'grace_days': 200},
    'RB': {'included': [1, 5, 10], 'grace_days': 200},
    'HC': {'included': [1, 5, 10], 'grace_days': 200},
    'FU': {'included': [1, 5, 9], 'grace_days': 200},
    'RU': {'included': [1, 5, 9], 'grace_days': 200},
    'SP': {'included': [1, 5, 9], 'grace_days': 200},
    # INE
    'SC': {'grace_days': 60},
    'NR': {'grace_days': 60},
    # CZC
    'CF': {'included': [1, 5, 9], 'grace_days': 200},
    'SR': {'included': [1, 5, 9], 'grace_days': 200},
    'OI': {'included': [1, 5, 9], 'grace_days': 200},
    'RM': {'included': [1, 5, 9], 'grace_days': 200},
    'CY': {'included': [1, 5, 9], 'grace_days': 200},
    'AP': {'included': [1, 5, 9], 'grace_days': 200},
    'CJ': {'included': [1, 5, 9], 'grace_days': 200},
    'TA': {'included': [1, 5, 9], 'grace_days': 200},
    'MA': {'included': [1, 5, 9], 'grace_days': 200},
    'FG': {'included': [1, 5, 9], 'grace_days': 200},
    'ZC': {'included': [1, 5, 9], 'grace_days': 200},
    'SF': {'included': [1, 5, 9], 'grace_days': 200},
    'SM': {'included': [1, 5, 9], 'grace_days': 200},
    'WR': {'included': [1, 5, 9], 'grace_days': 200},
    # CFE
    'IF': {},
    'IH': {},
    'IC': {},
    'T': {},
    'TS': {},
    'TF': {},
}


def worker(rs, f, debug=False):
    csvfile = open(os.path.expanduser(f'~/tmp/{rs}.csv'), 'w', newline='')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["Date Time", "Open", "High", "Low", "Close", "Volume", "Adj Close", 'open_interest'])

    datetime_format = "%Y-%m-%d %H:%M:%S"

    def on_bar(dateTime, value):
        bar_ = value[rs]
        csvwriter.writerow([
            dateTime.strftime(datetime_format),
            bar_.getOpen(),
            bar_.getHigh(),
            bar_.getLow(),
            bar_.getClose(),
            bar_.getVolume(),
            bar_.getAdjClose(),
            bar_.getExtraColumns()['open_interest'],
        ])

    feed = Feed(f)

    if debug:
        feed.setDebugMode(debug)

    feed.getNewValuesEvent().subscribe(on_bar)

    feed.loadBars(rs, **TRADED_INSTRUMENTS[rs])

    disp = dispatcher.Dispatcher()
    disp.addSubject(feed)
    disp.run()

    csvfile.close()


def main():
    parser = argparse.ArgumentParser(description="barfeed to csv")

    parser.add_argument("--rs", required=False, default=None, help="root symbol for futures")
    parser.add_argument("--f", required=True, type=int, help="86400=day or 60=minute")
    parser.add_argument("--debug", required=False, type=bool, help="debug or not")
    args = parser.parse_args()

    if args.rs:
        worker(args.rs, args.f, args.debug)
    else:
        for rs in TRADED_INSTRUMENTS.keys():
            worker(rs, args.f, args.debug)


if __name__ == "__main__":
    main()
