from mrjob.job import MRJob
import time

class Gas(MRJob):
#mapper job
    def mapper(self,_,line):
        try:
            fields = line.split(',')
            value = float(fields[5])
            block_timestamp  = time.localtime(float(fields[6]))
            if len(fields) == 7:#transactions dataset
                yield ((date.tm_mon,date.tm_year),(1,value))    

        except:
            pass
#combiner job
    def combiner(self,key,val):
        count = 0
        total = 0
        for v in val:
            count+=v[0]
            total+=v[1]
        yield (key,(count,total))
#reducer job
    def reducer(self,key,val):
        count = 0
        total = 0
        for v in val:
            count+=v[0]
            total+=v[1]
        yield (key,(total/count))


if __name__=='__main__':
    Gas.run()