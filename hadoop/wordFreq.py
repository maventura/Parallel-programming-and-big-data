from  mrjob.job  import  MRJob
import string

class  MRCharCount(MRJob ):
	def mapper(self , _, line):
		for x in string.punctuation:
			line = line.replace(x, ' ')
		words = line.split() #Aca el mapper crea muchas clases, bueno o malo?
		for w in words:
			self.increment_counter('group', 'total_words', 1)
			yield w, 1

	def reducer(self , key , values ):
		yield key , sum(values)
if __name__ == '__main__':
	print 'Starting map-reduce job'
	job = MRCharCount(args=['poem.txt'])
	runner = job.make_runner()
	runner.run()
	tmp_output = []
	count = runner.counters()
	for line in runner.stream_output():
		word, app = job.parse_output_line(line)
		total = count[0]['group']['total_words']
		print (word, app/float(total))
