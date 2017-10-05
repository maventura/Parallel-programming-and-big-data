from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSimpleDegree(MRJob):

    def sumList(aa,bb):
        res = []
        for a, b in zip(aa,bb):
            res.append(a+b)
        return res
        
    def mapper(self, _, line):
        (node_a, node_b) = line.replace('"','').split(",")
        if node_a < node_b:
            yield([node_a, node_b], 1)
        else if node_b > node_a:
            yield([node_b, node_a], 1)

    def reducer_aggregate(self, key, values):
          yield key[0] + key[1], [1 , 1]

    def reducer_sum(self, key, values):
            yield key, int(sumList(values))

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer_aggregate),
            MRStep(reducer = self.reducer_sum)
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt', 'w')
    MRSimpleDegree.run()


