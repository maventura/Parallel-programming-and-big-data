from pyspark import SparkContext
import sys

def splitToOrderedTuple(x, sep):
	r = x.split(sep)
	r.sort()
	return (r[0], r[1])

sc = SparkContext()
sc.setLogLevel("ERROR")

edges = sc.textFile(sys.argv[1])

edges = edges.map(lambda x: splitToOrderedTuple(x, ','))
edges = edges.distinct() 
edges = edges.flatMap(lambda x: x)

print(edges.collect())

out = []
print(' ___________________________________')
print('|              RESULTS              |')
print('|___________________________________|')

print('output', out)
