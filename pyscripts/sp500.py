import csv
from collections import namedtuple
from datetime import datetime
import itertools as it
import functools as ft


class DataPoint(namedtuple('DataPoint', ['date', 'value'])):
    __slots__ = ()

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __gt__(self, other):
        return self.value > other.value


def read_prices(csvfile, _strptime = datetime.strptime):
    with open(csvfile) as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield DataPoint(_strptime(row['Date'], '%Y-%m-%d').date(),
                            float(row['Adj Close']))


def consecutive_positives(sequence, zero=0):
    def _consecutives():
        for itr in it.repeat(iter(sequence)):
            yield tuple(it.takewhile(lambda x: x > zero,
                                it.dropwhile(lambda x: x <= zero, itr)))

    return it.takewhile(lambda x: len(x) > 0, _consecutives())


if __name__ == '__main__':
    prices = tuple(read_prices('./SP500.csv'))
    gains = tuple(DataPoint(today.date, (100 * (today.value/yesterday.value - 1.)))
                    for (yesterday, today) in zip(prices, prices[1:]))
    zdp = DataPoint(None, 0)
    max_gain = ft.reduce(max, it.filterfalse(lambda x: x <= zdp, gains), zdp)
    max_loss = ft.reduce(min, it.filterfalse(lambda x: x > zdp, gains), zdp)

    print('Maximum daily gain {gain:.2f}% occured on {day!s}'
            .format(gain=max_gain.value, day=max_gain.date))
    print('Maximum daily loss {loss:.2f}% occured on {day!s}'
            .format(loss=max_loss.value, day=max_loss.date))

    growth_streaks = consecutive_positives(gains, zdp)
    longest = ft.reduce(lambda x, y: x if len(x) > len(y) else y, growth_streaks)
    print('Longest growhth streak lasted {:d} days, from {start!s} to {end!s}'
            .format(len(longest), start=longest[0].date, end=longest[-1].date))
