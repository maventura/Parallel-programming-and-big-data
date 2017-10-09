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
print(edges.collect())
edges = edges.map(lambda x: splitToOrderedTuple(x, ','))
print(edges.collect())
edges = edges.distinct() 
print(edges.collect())
if not self_loops:
	edges = edges.filter(lambda x: x[0] != x[1])
edges = edges.map(lambda x: )

print(' _____________________________________________________________________________________  ')
print('|                                                                                     | ')
print('|                                   R E S U L T S                                     ||')
print('|_____________________________________________________________________________________||')
print(' \_____________________________________________________________________________________|')

print(edges.collect())