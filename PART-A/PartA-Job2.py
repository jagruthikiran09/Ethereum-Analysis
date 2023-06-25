from mrjob.job import MRJob
import re 
import time
import statistics
class PartA2(MRJob):
#mapper job
	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:#transactions dataset
				block_timestamp = int(fields[6])#time in milliseconds
				gas_price = int(fields[5])#gas price from the transaction dataset
				month = time.strftime("%m", time.gmtime(block_timestamp))#converting time into month
				year = time.strftime("%y", time.gmtime(block_timestamp))#converting time into year
				yield((month,year),(gas_price,1))
		except:
			pass
#redcuer1 job
	def reducer1(self, date, price):
		average_value = 0
		count = 0
		for x, y in price:
			average_value = (average_value*count+x*y)/(count + y)
			count = count + y
		return(date, (average_value,count))
#combiner job
	def combiner(self, date, price):
		yield self.reducer1(date,price)
#reducer job
	def reducer(self,date,price):     
		date, (average_value,count) = self.reducer1(date,price)
		yield(date,average_value)

if __name__ == '__main__':
	PartA2.run()
