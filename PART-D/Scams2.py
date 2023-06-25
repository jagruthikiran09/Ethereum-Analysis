from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class scam2(MRJob):
#mapper1 job
    def mapper1(self, _, lines):
        try:
            fields = lines.split(',')
            if len(fields) == 7:#transactions dataset
                address = fields[2]
                yield (address, (1, 0))
            else:
                line = json.loads(lines)#scams.json dataset
                keys = line['result']

                for i in keys:
                    record = line['result'][i]
                    category = record['category']
                    addresses = record['addresses']
                    status = record['status']

                    for j in addresses:
                        yield (j, (2, category, status))
        except:

            pass
#redcuer1 job
    def reducer1(self, key, values):
        tvalue = 0
        category = None
        status = None
        for k in values:
            if k[0] == 1:
                tvalue = tvalue + k[0]
            else:
                category = k[1]
                status = k[2]
        if category is not None and status is not None:
            yield ((status, category), tvalue)
#mapper2 job
    def mapper2(self, key, value):
        yield (key, value)
#reducer2 job
    def reducer2(self, key, value):
        yield (key, sum(value))

    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper=self.mapper2, reducer=self.reducer2)]


if __name__ == '__main__':
    scam2.run()

