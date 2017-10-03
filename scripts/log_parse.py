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
            #print(list)
            ip, date, timezone, get, name, protocol, ans = list
            t = time.mktime(time.strptime(date, "%d/%b/%Y:%H:%M:%S"))
            yield ip, [t, name]
            #para el segundo emito tiempo y pagina
            #envio ip,compotamiento como clave, y 1 como valor.
            #hago sum.

    def reducer_get_data(self , key , values):
        #me da un key y un generador de valores
        #con todos los yield de ese values
        max_time = 250
        first = True
        session_no = 0
        tmp = 0
        behaviour = []
        behaviour_list = []
        for v in values:
            time, name = v

            if not first:
                if time - tmp > max_time:
                    behaviour_list.append(behaviour)
                    behaviour = []
                else:
                    behaviour.append(name)

            else:
                behaviour.append(name)
                first = False
                tmp = time

        for b in behaviour_list:
            yield [key, b], 1
            

            

    def reducer_sum(self , key , values ):
            print(key, sum(values))




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


