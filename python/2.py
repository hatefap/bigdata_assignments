from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class CountWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.normalizeAndSplit, reducer=self.countWord),
            MRStep(mapper=self.agg, reducer=self.topTen)
        ]

    def normalizeAndSplit(self, _, line):
        results = re.findall(r'\w+', line.lower())
        for s in results:
            yield s, 1

    def countWord(self, key, values):
        yield key, sum(values)

    def agg(self, key, value):
        yield None, ((key, value))

    def topTen(self, _, word_count):
        restuls = sorted(word_count, key=lambda tuple: tuple[1], reverse=True)[:10]
        for word, count in restuls:
            yield str(word), count



if __name__ == "__main__":
    CountWord.run()



