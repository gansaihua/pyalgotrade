import os
import argparse
from pyalgotrade.barfeed.futuresfeed import get_pricing


def main():
    parser = argparse.ArgumentParser(description="barfeed to csv")

    parser.add_argument("--rs", required=True, default=None, help="root symbol for futures")
    parser.add_argument("--frequency", required=True, help="day or minute")
    parser.add_argument("--adjustment", required=False, default=None,
                        help="`add`, `mul` or None are supported.")
    args = parser.parse_args()

    df = get_pricing(args.rs, args.frequency, adjustment=args.adjustment)
    df.to_csv(os.path.expanduser(f'~/tmp/{args.rs}_{args.frequency}_{args.adjustment}.csv'))


if __name__ == "__main__":
    main()
    # get_pricing('RU', 'day', adjustment='add')
