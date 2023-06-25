from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class PARTDscams(MRJob):
#mapper1 job
	def mapper1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:#transactions dataset
				address = fields[2]
				value = float(fields[3])
				yield address, (value,0)
			
			else:
				line = json.loads(lines)#scams.json dataset
				keys = line["result"]

				for i in keys:
					record = line["result"][i]
					category = record["category"]
					addresses = record["addresses"]

					for j in addresses:
						yield j, (category,1)

		except:
			pass
#reducer1 job
	def reducer1(self, key, values):
		total_value=0
		category=None

		for k in values:
			if k[1] == 0:
				total_value = total_value + k[0]
			else:
				category = k[0]
		if category is not None:
			yield category, total_value
#mapper2 job
	def mapper2(self,key,value):
		yield(key,value)
 #reducer2 job       
	def reducer2(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTDscams.run()
