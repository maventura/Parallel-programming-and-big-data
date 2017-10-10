from  mrjob.job  import  MRJob
import string

class  MRDegreeEdgeCount(MRJob ):
	def mapper(self , _, line):
		l = line
		for x in string.punctuation:
			l = l.replace('"', ' ')
		ws = l.split(',') #Aca el mapper crea muchas clases, bueno o malo?
		w0 = ws[0]
		w1 = ws[1]
		yield w0, [(w0, w1)]
		yield w1, [(w0, w1)]

	def reducer(self , key , values ):
		res = []
		for x in values:
			res = res + values.next()
		yield key , res

	def reducer2(self , key , values ):
		res = []
		for x in values:
			res = res + values.next()
		yield key , res



def steps(self):
		return[
			MRStep(mapper = self.mapper,
				reducer = self.reducer)#,
			#MRStep(reducer = self.reducer2)
		]


if __name__ == '__main__':
	print 'Starting map-reduce job'
	job = MRDegreeEdgeCount(args=['../data/graphs/g1.txt'])
	runner = job.make_runner()
	runner.run()
	tmp_output = []
	for line in runner.stream_output():
		print (line)
