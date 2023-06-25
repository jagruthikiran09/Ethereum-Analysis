from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTB(MRJob):
#mapper job
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:#transactions dataset
				address = fields[2]
				value = int(fields[3])
				yield address, (1,value)
			elif len(fields) == 5:#contracts dataset
				address1 = fields[0]
				yield address1, (2,1)
		except:
			pass
#reducer1 job
	def reducer1(self, word, counts):
		x = False
		values = []
		for i in counts:
			if i[0]==1:
				values.append(i[1])
			elif i[0] == 2:
				x = True
		if x:
			yield word, sum(values)
#mapper2 job
	def mapper2(self, word,counts):
		yield None, (word,counts)
#sorting the most popular services and yielding top 10 services
	def reducer2(self, _, word):
		sorted_data = sorted(word, reverse = True, key = lambda y: y[1])
		for i in sorted_data[:10]:
			yield i[0], i[1]

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTB.run()
			
