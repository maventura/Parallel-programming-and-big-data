from  mrjob.job  import  MRJob
import string
class  MRCharCount(MRJob ):
	def mapper(self , _, line):
		for x in string.punctuation:
			line = line.replace(x, ' ')
		words = line.split() #Aca el mapper crea muchas clases, bueno o malo?
		for w in words:
			yield w, 1
	def reducer(self , key , values ):
		yield key , sum(values)
if __name__ == '__main__':
	MRCharCount.run()