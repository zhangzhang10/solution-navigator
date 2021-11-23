from collections import namedtuple
import csv
import statistics
import datetime
import itertools as it

class Event(namedtuple('Event', ['stroke', 'name', 'time'])):
    __slots__ = ()

    def __lt__(self, other):
        return self.time < other.time


def read_events(csvfile, _strptime = datetime.datetime.strptime):
    def _median(times):
        return statistics.median(_strptime(time, '%M:%S:%f').time() for time in times)

    with open(csvfile) as infile:
        reader = csv.DictReader(infile, ['Event', 'Name','Stroke'],
                restkey='Times')
        next(reader)
        for row in reader:
            yield Event(row['Stroke'], row['Name'], _median(row['Times']))


def sort_and_group(iterable, key=None):
    return it.groupby(sorted(iterable, key=key), key=key)


def grouper(inputs, n, fillvalue=None):
    itrs = [iter(inputs)] * n
    return it.zip_longest(*itrs, fillvalue=fillvalue)


if __name__ == '__main__':
    events = tuple(read_events('./swimmers.csv'))
    for stroke, evts in sort_and_group(events, key=lambda x: x.stroke):
        evts_by_name = sort_and_group(evts, key=lambda x: x.name)
        best_times = (min(ts) for _, ts in evts_by_name)
        sorted_by_time = sorted(best_times)
        group_by_4 = grouper(sorted_by_time, 4)
        teams = zip(('Team A', 'Team B'), it.islice(group_by_4, 2))
        for team, swimmers in teams:
            print('{stroke} {team}: {names}'.format(
                stroke=stroke.capitalize(),
                team=team,
                names=','.join(swimmer.name for swimmer in swimmers)))
