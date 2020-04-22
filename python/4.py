from mrjob.job import MRJob
from mrjob.step import MRStep
import apache_log_parser as parser
import ntpath


line_parser = parser.make_parser("%h %t \"%r\" %>s %b")
TOKEN = '#'


class VisitCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.extractIpAndDate, reducer=self.countVisitPerDay),
            MRStep(mapper=self.extractVisitPerMonth, reducer=self.countVisitPerMonth)
        ]

    def extractIpAndDate(self, _, line):
        parsed_line = line_parser(line)
        ip = parsed_line['remote_host'].split()[0]
        date = parsed_line['time_received_datetimeobj']
        year = str(date.year)
        month = str(date.month)     
        day = str(date.day)
        key = ip + TOKEN + year + TOKEN + month + TOKEN + day
        yield key, 1

    def countVisitPerDay(self, key, values):
        yield key, 1

    def extractVisitPerMonth(self, key, value):
        ipYearMonth = TOKEN.join(key.split(TOKEN)[:3])
        yield ipYearMonth, 1

    def countVisitPerMonth(self, ipYearMonth, values):
        parts = ipYearMonth.split(TOKEN)
        ip = parts[0]
        yearMonth = '/'.join(parts[1:3])
        yield ip + '-' + yearMonth, sum(values)





if __name__ == "__main__":
    VisitCount.run()

