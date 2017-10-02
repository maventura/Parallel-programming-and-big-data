from mrjob.job import MRJob
from mrjob.step import MRStep
def sumList(aa,bb):
    res = []
    for a, b in zip(aa,bb):
        res.append(a+b)
    return res

class MRSimpleDegree(MRJob):

    def mapper(self, _, line):
        (node_a, node_b) = line.replace('"','').split(",")
        if node_a < node_b:
            yield([node_a, node_b], 1)
        else if node_b > node_a:
            yield([node_b, node_a], 1)

    def reducer(self, key, values):
          yield key[0] + key[1], [1 , 1]

    def reducer_2(self, key, values):
            yield key, int(sum(values))

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.reducer_2)
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt', 'w')
    MRSimpleDegree.run()


