from pyspark import SparkContext

sc = SparkContext()

data = sc.textFile('../data/buscon_de_quevedo.txt')

len_lines_rdd = data.map(lambda x: len(x.split()))


char_count = len_lines_rdd.sum()

print(' ___________________________________')
print('|              RESULTS              |')
print('|___________________________________|')

print('word_count', char_count)
