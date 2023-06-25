from mrjob.job import MRJob
import re
import time
import statistics

class PARTA(MRJob):
#mapper job	
	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:#transactions dataset
				block_timestamp = int(fields[6])#timestamp
				month = time.strftime("%m", time.gmtime(block_timestamp))#converting timestamp into month
				year = time.strftime("%y", time.gmtime(block_timestamp))#converting timestamp into year
				yield ((month,year), 1)
		except:
			pass
#combiner job
	def combiner(self,key,values):
		yield(key,sum(values))	
#reducer job
	def reducer(self,key,values):
		yield(key,sum(values))

if __name__ == '__main__':
	PARTA.run()
