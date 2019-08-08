from pyalgotrade import stratanalyzer
from pyalgotrade import dataseries

import datetime


class PnL(stratanalyzer.StrategyAnalyzer):
    """A :class:`pyalgotrade.stratanalyzer.StrategyAnalyzer` that record pnl for the portfolio."""

    def __init__(self, maxLen=None):
        super(PnL, self).__init__()
        self.__nav = dataseries.SequenceDataSeries(maxLen=maxLen)
        self.__pnl = dataseries.SequenceDataSeries(maxLen=maxLen)

        self.prev = None

    def calculateEquity(self, strat):
        return strat.getBroker().getEquity()

    def beforeOnBars(self, strat, bars):
        equity = self.calculateEquity(strat)
        if self.prev is None:
            nav = equity
            pnl = 0
        else:
            nav = equity
            pnl = equity - self.prev

        self.prev = equity

        self.__nav.appendWithDateTime(bars.getDateTime(), nav)
        self.__pnl.appendWithDateTime(bars.getDateTime(), pnl)

    def getNaV(self):
        """Returns pnl."""
        return self.__nav

    def getPnL(self):
        """Returns pnl."""
        return self.__pnl
