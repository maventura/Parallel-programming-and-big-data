from  mrjob.job  import  MRJob

class  MRCharCount(MRJob ):
	def mapper(self , _, line):
		yield ('map' in line.lower()), 1

	def reducer(self , key , values ):
		yield key, sum(values)

if __name__ == '__main__':
	MRCharCount.run()