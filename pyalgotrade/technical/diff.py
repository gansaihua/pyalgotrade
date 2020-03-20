from pyalgotrade import technical


class DiffEventWindow(technical.EventWindow):
    def __init__(self, windowSize):
        super(DiffEventWindow, self).__init__(windowSize)

    def getValue(self):
        ret = None
        if self.windowFull():
            prev = self.getValues()[0]
            actual = self.getValues()[-1]
            if actual is not None and prev is not None:
                ret = float(actual - prev)
        return ret


class Diff(technical.EventBasedFilter):
    """Difference filter

    :param dataSeries: The DataSeries instance being filtered.
    :type dataSeries: :class:`pyalgotrade.dataseries.DataSeries`.
    :param valuesAgo: The number of values back that a given value will compare to. Must be > 0.
    :type valuesAgo: int.
    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """

    def __init__(self, dataSeries, valuesAgo, maxLen=None):
        assert(valuesAgo > 0)
        super(Diff, self).__init__(dataSeries,
                                   DiffEventWindow(valuesAgo + 1),
                                   maxLen)
