import io
import datetime

from tensorflow.compat.v1 import Summary
from tensorflow.compat.v1.summary import FileWriter
from tensorflow.python.util.nest import is_sequence

from pyalgotrade.plotter import StrategyPlotter
from pyalgotrade.stratanalyzer.returns import Returns
from pyalgotrade.stratanalyzer.sharpe import SharpeRatio
from pyalgotrade.stratanalyzer.drawdown import DrawDown
from pyalgotrade.stratanalyzer.trades import Trades


class TensorBoard(object):
    """
    log_dir: the path of the directory where to save the log
        files to be parsed by tensorboard
    max_queue: Maximum number of summaries or events pending to be
               written to disk before one of the 'add' calls block.
               [default 10]
    flush_secs: How often, in seconds, to flush the added summaries
        and events to disk. [default 120]
    """
    def __init__(self, log_dir='./logs', max_queue=10, flush_secs=120):
        self.log_dir = log_dir
        self.writer = FileWriter(self.log_dir, max_queue=max_queue,
                                 flush_secs=flush_secs, graph_def=None)

    def log_algo(self, algo, epoch=None):
        if epoch is None:
            epoch = datetime.date.toordinal(algo.getCurrentDateTime())

        logs = dict()
        logs['portfolio/nav'] = algo.getResult()
        logs['portfolio/cash'] = algo.getBroker().getCash()

        for analyzer in algo.getAnalyzers():
            if isinstance(analyzer, Returns):
                logs['analyzer/return'] = analyzer.getReturns()[-1]
                logs['analyzer/cumulative return'] = analyzer.getCumulativeReturns()[-1]
            elif isinstance(analyzer, SharpeRatio):
                logs['analyzer/sharpe ratio'] = analyzer.getSharpeRatio(0)
            elif isinstance(analyzer, DrawDown):
                logs['analyzer/max drawdown'] = analyzer.getMaxDrawDown()
                logs['analyzer/longest drawdown duration'] = analyzer.getLongestDrawDownDuration().days
            elif isinstance(analyzer, Trades):
                logs['analyzer/profitable trade'] = analyzer.getProfitableCount()
                logs['analyzer/unprofitable trade'] = analyzer.getUnprofitableCount()
                logs['analyzer/even trade'] = analyzer.getEvenCount()

        for instrument in algo.getFeed().getKeys():
            logs['price/'+instrument] = algo.getLastPrice(instrument)
            logs['share/'+instrument] = algo.getBroker().getShares(instrument)
            logs['active order/'+instrument] = len(algo.getBroker().getActiveOrders(instrument))

        self.log_values(epoch, dict=logs)

    def log_values(self, step, tags=None, values=None, dict=None):
        if dict is not None:
            assert tags is None and values is None
            tags = dict.keys()
            values = dict.values()
        else:
            if not is_sequence(tags):
                tags, values = [tags], [values]
            elif len(tags) != len(values):
                raise ValueError('tag and value have different lenghts:'
                                 ' {} vs {}'.format(len(tags), len(values)))

        for t, v in zip(tags, values):
            summary = Summary.Value(tag=t, simple_value=v)
            summary = Summary(value=[summary])
            self.writer.add_summary(summary, step)
        self.writer.flush()

    def log_matplot(self, tag, buffer_or_plt):
        if isinstance(buffer_or_plt, StrategyPlotter):
            buffer = io.BytesIO()
            buffer_or_plt.savePlot(buffer)
        else:
            buffer = buffer_or_plt

        summary = Summary.Image(encoded_image_string=buffer.getvalue())
        summary = Summary.Value(tag=tag, image=summary)
        summary = Summary(value=[summary])
        self.writer.add_summary(summary)
        self.writer.flush()
