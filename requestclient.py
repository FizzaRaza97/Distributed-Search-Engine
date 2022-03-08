import sys
import socket  
import timeit
import os
import Queue
import time

def displayresults(rdata):
	global n
	global query
	for result in rdata:
		if '!!$^' in result:
		    rdata[rdata.index(result)] = result.split('!!$^')
	for k in rdata:
		rdata[rdata.index(k)] = filter(None,k)
	rdata = filter(None,rdata)
	for i in xrange(0,len(rdata),2):
		for j in rdata[i+1]:
			text = str(n)+': '+rdata[i]+': '+j+'\n'
			# print n,':',rdata[i],':', j,'\n'
			print(text.replace(query, '\033[44;33m{}\033[m'.format(query)))
			n+=1
		# except:
		# 	raw_input()
		# 	break

def main(argv):
        global n
        global query
        file_name=str(argv[0])   
        ip=str(argv[1])
        port=int(argv[2])  
        n = 0
        '''ip='127.0.0.1'
        port=8100'''
        myid = 0
        numpings = 0
        query = raw_input('Enter query: ')
        sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        m = '15440,0,8,'+query
        sock.sendto(m,(ip,port)) #Send query
        data,addr = sock.recvfrom(61439)
        data = data.split(',')
        myid = data[1]
        sock.setblocking(0)
        t0 = time.clock()
        while 1:
                try:
                        if data[0]=='15440':
                                # print data
                                task = data[2]
                                if task=='2':
                                        print 'Job ACK received from server'
                                if task=='4':
                                        print 'No More Results Found \n'
                                        numpings-=1
                                        break
                                elif task=='6':
                                        displayresults(data[6:])
                                        numpings-=1
                                elif task=='5':
                                        displayresults(data[6:])
                                        numpings-=1
                                        print 'Job complete \n'
                                        break
                        data,addr = sock.recvfrom(61439)
                        data = data.split(',')
                except socket.error as error:
                        print 'Waiting for message from server \n'
                        data = ['xx','xx','xx','x']
                        time.sleep(0.3)
                t1 = time.clock()
                print t1-t0
                if t1-t0>0.04:
                        if numpings <3:
                                m = '15440,'+str(myid)+',0,'+query
                                sock.sendto(m,(ip,port))
                                print 'Pinged server'
                                numpings+=1
                                t0 = time.clock()
                        else:
                                print "Server has crashed, Please try again later \n"
                                break

if __name__ == "__main__":
    main(sys.argv)





