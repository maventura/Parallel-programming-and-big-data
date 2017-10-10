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

cant1 = edges.map(lambda x: (x[0],x))
cant2 = edges.map(lambda x: (x[1],x))

res = sc.union([cant1,cant2])

res = res.filter(lambda x: type(x[1]) == tuple)

res = res.map(lambda x: (x[0],[x[1]]))
res = res.reduceByKey(lambda x, y: x+y)

def defCant(x):
	var = x[0]
	list = x[1]
	cants = []
	for e in list:
		if e[0] == var:
			cants = cants + [(len(list), 0)]
		else:
			cants = cants + [(0, len(list))]
	return (var,(list,cants))

res = res.map(lambda x: defCant(x))

def toTuples(x):
	lists = x[1]
	tmp = zip(lists[0], lists[1])
	res = []
	for a, b in tmp:
		res = res + [(a,b)]
	return tuple(res)

res = res.flatMap(lambda x: toTuples(x))


def reducePairs(x,y):
	return (x[0] + y[0], x[1] + y[1]) 


res = res.reduceByKey(lambda x, y: reducePairs(x, y))


print(' _____________________________________________________________________________________  ')
print('|                                                                                     | ')
print('|                                   R E S U L T S                                     ||')
print('|_____________________________________________________________________________________||')
print(' \_____________________________________________________________________________________|')

print(res.collect())
