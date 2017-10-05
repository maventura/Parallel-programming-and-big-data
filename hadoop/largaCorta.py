from  mrjob.job  import  MRJob

class  MRCharCount(MRJob ):

	def mapper(self , _, line):
		if len(line) > 10:
			yield "larga", 1
		else:
			yield "corta", 1
	def reducer(self , key , values):
		yield key , sum(values)
if __name__ == '__main__':
	MRCharCount.run()