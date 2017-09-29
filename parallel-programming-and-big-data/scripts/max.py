from  mrjob.job  import  MRJob

class  MRCharCount(MRJob ):
	def mapper(self , _, line):
		yield "max", int(line)
	def reducer(self , key , values ):
		max = int(next(values))
		for x in values: #Itera bien aunque sea un stream.
			if x > max:
				print (max, type(max), x, type(x))
				max = x
		yield key, max

if __name__ == '__main__':
	MRCharCount.run()