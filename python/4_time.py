from mrjob.job import MRJob
from mrjob.step import MRStep
import apache_log_parser as parser
import ntpath
import datetime


line_parser = parser.make_parser("%h %t \"%r\" %>s %b")
TOKEN = '#'
SESSION_BREAK = 30*60

class Session:
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end

class VisitTime(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.extractIpAndDate, reducer=self.countVisitPerDay),
            MRStep(reducer=self.topTen)
        ]

    def extractIpAndDate(self, _, line):
        parsed_line = line_parser(line)
        ip = parsed_line['remote_host'].split()[0]
        year = parsed_line['time_received_datetimeobj'].year
        yield ip + '#' + str(year), str(parsed_line['time_received_datetimeobj'])

    def countVisitPerDay(self, key, values):
        values = [datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in values]
        values.sort()
        result = []
        firstOfSession = values[0]
        for i in range(1, len(values)):
            if (values[i] - values[i-1]).total_seconds() >= SESSION_BREAK:
                result.append(Session(firstOfSession, values[i - 1]))
                firstOfSession = values[i]
        if (values[len(values) - 1] - firstOfSession).total_seconds() < SESSION_BREAK:
            result.append(Session(firstOfSession, values[len(values) - 1]))

        totalTimeInSeconds = 0
        for session in result:
            totalTimeInSeconds += (session.end - session.begin).total_seconds()
        year = key.split('#')[1]
        ip = key.split('#')[0]
        yield str(year), ip + '#' + str(totalTimeInSeconds/60)


    def topTen(self, key, values):
        result = []
        for v in values:
            parts = v.split("#")
            ip = parts[0]
            time = parts[1]
            result.append((ip, float(time)))
        result = sorted(result, key=lambda x:float(x[1]), reverse=True)
        for t in result[0:10]:
            yield key, (t[0], "%.2f" % t[1] + ' minutes')






if __name__ == "__main__":
    VisitTime.run()
