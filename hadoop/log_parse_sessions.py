from mrjob.job import MRJob
from mrjob.step import MRStep
import string
import time

SORT_VALUES = True

class  MRSession(MRJob ):
    def mapper(self , _, line):
        if len(line) > 10:
            line = line.replace('"','').replace(' - ',' ').replace('-','.')
            line = line.replace('. ',' ').replace(' .',' ').replace('[','')
            line = line.replace(']','').replace('  ',' ').replace('  ',' ')
            list = line.split(" ")
            list = list[:7]
            print(list)
            ip, date, timezone, get, name, protocol, ans = list
            print(type(date))
            t = time.mktime(time.strptime(date, "%d/%b/%Y:%H:%M:%S"))
            yield ip, t
            #para el segundo emito tiempo y pagina
            #envio ip,compotamiento como clave, y 1 como valor.
            #hago sum.

    def reducer_get_data(self , key , values ):
        #me da un key y un generador de valores
        #con todos los yield de ese values
        max_time = 3
        first = True
        cant_sessions = 0
        tmp = 0
        for v in values:
            if not first:
                if v-tmp > max_time:
                    yield key, 1
            else:
                yield key, 1
                first = False
                tmp = v

    def reducer_sum(self , key , values ):
        yield key , sum(values)




    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer_get_data),
            MRStep(reducer = self.reducer_sum)
        ]



if __name__ == '__main__':
    print 'Starting map-reduce job'
    job = MRSession(args=['apache_http_server_log.txt'])
    runner = job.make_runner()
    runner.run()
    for line in runner.stream_output():
        a, b = job.parse_output_line(line)
        print (a, b)


