from pyspark import SparkContext

sc = SparkContext()

data = sc.textFile('../data/buscon_de_quevedo.txt')

words = data.flatMap(lambda x: x.split())

word_cant = words.map(lambda x: (x,1))




res = word_cant.reduceByKey(lambda x, y: (x.first, x.second + y.second))
#map usa una sola entrada. Pero puede tener varias de salida.



out = word_cant.collect()

print(' ___________________________________')
print('|              RESULTS              |')
print('|___________________________________|')

print('output', out)
