import string
import time

string = '88.198.56.239 - - [26/Apr/2015:08:05:09 +0200] "GET a.html HTTP/1.1" 200'
line = string.replace('"','').replace('-','').replace('[','').replace(']','').replace('   ',' ').split(" ")
print(line)
ip, date, date, get, name, protocol, ans = line
print(name)


#datetime.strptime('07/28/2014 18:54:55.099000', '%m/%d/%Y %H:%M:%S.%f')

date = '26/Apr/2015:08:05:09'
t = time.mktime(time.strptime(date, '%d/%b/%Y:%H:%M:%S'))
print(t)

