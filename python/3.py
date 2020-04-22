from mrjob.job import MRJob
from mrjob.step import MRStep
import apache_log_parser as parser
import ntpath


# install pip install apache-log-parser

line_parser = parser.make_parser("%h %t \"%r\" %>s %b")

FAV_IMAGE_PATH = '/images/newspics'




class ImageCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.extractImageAndYear, reducer=self.countImageInYear),
            MRStep(mapper=self.extractYearAndImageCount, reducer=self.topTenInEachYear)
        ]

    def extractImageAndYear(self, _, line):
        parsed_line = line_parser(line)
        request_url = parsed_line['request_url'].lower()
        if (request_url.startswith(FAV_IMAGE_PATH, 0, len(request_url)) and (
                request_url.endswith('.png') or request_url.endswith('jpg'))):
            year = parsed_line['time_received_datetimeobj'].year
            imagename = ntpath.basename(request_url)
            key = str(year) + "_" + imagename
            yield key, 1

    def countImageInYear(self, yearimage, values):
        yield yearimage, sum(values)

    def extractYearAndImageCount(self, yearimage, count):
        year = yearimage[0:4]
        imagename = yearimage[5:]

        yield year, (imagename, count)


    def topTenInEachYear(self, year, imgcntlist):
        restuls = sorted(imgcntlist, key=lambda tuple: tuple[1], reverse=True)[:10]
        yield year, restuls


if __name__ == "__main__":
    ImageCount.run()


