from pyspark import SparkContext
import sys

self_loops = False;

def splitToOrderedTuple(x, sep):
	r = x.split(sep)
	r.sort()
	return (r[0], r[1])

sc = SparkContext()
sc.setLogLevel("ERROR")

edges = sc.textFile(sys.argv[1])
edges = edges.map(lambda x: splitToOrderedTuple(x, ','))
edges = edges.distinct() 
if not self_loops:
	edges = edges.filter(lambda x: x[0] != x[1])
edges = edges.flatMap(lambda x: x).map(lambda x: (x,1))
edges = edges.reduceByKey(lambda x, y: x + y)
print(' ___________________________________ ')
print('|                                   |')
print('|              RESULTS              |')
print('|___________________________________|')

print(edges.collect())
