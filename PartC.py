from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTC(MRJob):
#mapper1 job
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:#contracts dataset
				miner = fields[2]#miner field from the contracts dataset
				size = fields[4]#size field from the contracts dataset
				yield (miner, size)

		except:
			pass
#reducer1 job
	def reducer1(self, miner, size):
		try:
			yield(miner, sum(size))

		except:
			pass

#mapper2 job
	def mapper2(self, miner, aggsize):
		try:
			yield(None, (miner,aggsize))
		except:
			pass
#top 10 most active miners
	def reducer2(self, _, minersize):
		try:
			sorted_values = sorted(minersize, reverse = True, key = lambda b:b[1])
			for i in sorted_values[:10]:
				yield(i[0],i[1])
		except:
			pass
	

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTC.run()	
	
		

