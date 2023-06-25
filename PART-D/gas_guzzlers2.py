from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import os
import json

class Gas2(MRJob):

#mapper1 job	
	def mapper1(self, _,line):
		fields = line.split(",")
		try:
			if len(fields) == 9:#blocks dataset
				block_number = float(fields[0])
				block_difficulty = float(fields[3])
				block_gas_used = float(fields[6])
				timef = int(fields[7])
				month = time.strftime("%m", time.gmtime(timef))   
				year = time.strftime("%Y", time.gmtime(timef)) 
				timekey1 = (year, month)
				value_tuple1 = (0, timekey1, block_gas_used, block_difficulty)
				yield (block_number, value_tuple1)

			elif len(fields) == 5:#contracts dataset
				address = fields[0]
				block_number1 = float(fields[3])
				yield (block_number1, (1,"In"))
		except:
			pass
#reducer1 job
	def reducer1(self,word,counts):

		val_list = []
		x = False

		for each in counts:

			if each[0]==0 :
				val_list.append((each[1],each[2], each[3]))
			elif each[0]==1:
				x = True
				

		if x:
			if val_list :
				for each in val_list:
					yield (each[0],(each[1],each[2]))
#mapper2 job
	def mapper2(self, key,values):
		yield (key, (values[0], values[1], 1))
#reducer2 job
	def reducer2(self,word,counts):

		i = 0
		total_difficulty = 0
		total_gas_used = 0
		for each in counts :
			total_gas_used += each[0]
			total_difficulty += each[1]
			i += each[2]

		average_difficulty = float(total_difficulty)/ float(itc)
		average_gas_used = float(total_gas_used)/ float(itc)

		yield(word,(average_gas_used, average_difficulty))

										
#combiner job
	def combiner(self,word,counts):
	
		i = 0
		total_difficulty = 0
		total_gas_used = 0
		for each in counts :
			total_gas_used += each[0]
			total_difficulty += each[1]
			i += each[2]

		intermediate_values = (total_gas_used, total_difficulty, i)

		yield(word,intermediate_values)

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, combiner = self.combiner2, reducer = self.reducer2)]




if __name__ == '__main__':

	Gas2.run()
