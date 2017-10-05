from pyspark import SparkContext

sc = SparkContext()

data = sc.textFile('../data/buscon_de_quevedo.txt')

len_lines_rdd = data.map(lambda x: len(x))


char_count = len_lines_rdd.sum()

print('______________RESULTS______________', char_count)

print('char_count', char_count)
