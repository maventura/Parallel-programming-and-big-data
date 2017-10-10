from __future__ import division
from pyspark import SparkContext

import sys

sc = SparkContext()
sc.setLogLevel("ERROR")

data = sc.textFile(sys.argv[1])

words = data.flatMap(lambda x: x.split())

word_cant = words.map(lambda x: (x,1))

total = words.count()

cant_by_word = word_cant.reduceByKey(lambda x, y: x + y)
#Si usara reduce normal se puede hacer lambda (None, x[1] + y[1])

freq = cant_by_word.map(lambda x: (x[0],x[1]/total))
#map usa una sola entrada. Pero puede tener varias de salida.

sortedFreq = freq.sortBy(lambda x: x[1], False)

out = freq.takeOrdered(5, lambda s: -1*s)

print(' ___________________________________')
print('|              RESULTS              |')
print('|___________________________________|')

print('output', out)
