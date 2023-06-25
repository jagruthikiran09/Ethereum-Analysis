from mrjob.job import MRJob
import time

class Fork(MRJob):
#mapper job
    def mapper(self,_,line):
        try:
            fields = line.split(',')
            gas_price = float(fields[5])
            block_timestamp= time.gmtime(float(fields[6]))#timestamp from the transactions dataset
            if len(fields) == 7:#transactions dataset
                if (date.tm_year== 2019 and date.tm_mon== 2):
                    yield ((date.tm_mday), (1, gas_price))
                #yield ((date.tm_mon,date.tm_year),gas_price)


        except:
            pass
#combiner job
    def combiner(self,key,values):
        count = 0
        total = 0
        for i in values:
            count+= i[0]
            total= i[1]

        yield (key,(count,total))
#reducer job
    def reducer(self,key,values):
        count1 = 0
        total1 = 0
        for j in values:
            count1 += j[0]
            total1 = j[1]

        yield (key, (count1,total1))

if __name__=='__main__':
    Fork.run()
