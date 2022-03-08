import sys
import socket  
import time
import os
import threading


def paths():
    allfiles = []
    for root,dirs,files in os.walk('database'):
        for f in files:
            allfiles.append(root+'/'+f)
        if not dirs:
            for dire in dirs:
                for x in os.listdir(dire):
                    allfiles.append(dirs+'/'+x)
    return allfiles

def dowork(data,addr): #Key of results dictionary is file name and value is an array of all lines that contain it
    print 'Doing Work'
    global currfile
    global allfiles
    global results
    global done
    global terminate
    global thread
    done = False
    string = data[5]
    startfile = int(data[3])
    currfile = startfile
    endfile = int(data[4])
    for i in range(startfile,endfile):
        fileres = []
        for line in open(allfiles[i],'r'):
            line = line.lower()
            line = line.replace('\n','')
            line = line.replace(',','')
            line = line.replace('!','')
            line = line.replace('$','')
            line = line.replace('^','')
            if string.lower() in line:
                fileres.append(line)
        if len(fileres)>0:
            results[(allfiles[i],i)] = fileres
        currfile+=1
        if terminate==True:
            thread.exit()
        if i%50==0:
            time.sleep(0.1)
    done = True

def main(argv):
    global currfile
    global allfiles
    global results
    global done
    global terminate
    global thread
    ip=str(argv[1])
    port=int(argv[2]) 
    '''ip='127.0.0.1'
    port=8100'''
    myid = 0

    results = {} #Key: (Filename,Indexoffile), Value: Results array of lines in the file GLOBAL
    allfiles = paths() #Paths to every searchable file in the database GLOBAL
    currfile = 0 #File currently being searched with reference to position of that file's path in allfiles GLOBAL
    done = False
    sf = 0
    ef = 0
    terminate = False

    # ACK of a results message will have code 9 and will contain the file range
    # Messages being sent can be stored in a dicyionary and,once ACK received, be deleted.
    # Key: last file number, Value: ([time of sending], [array of messages])

    sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    m = '15440,0,1,xx,xx,xx,x'
    sock.sendto(m,(ip,port)) #Request to join

    data,addr = sock.recvfrom(61439) #Response from the server
    data = data.split(',')
    print 'received: ',data
    myid = data[1]
    j = False
    while 1:
        print data
        task = data[2]
        thread = threading.Thread(target = dowork,args = (data,addr))
        if data[0]=='15440':
            if task=='2': #Job
                thread.start()
                j = True
                sf = data[3]
                ef = data[4]
                m = '15440,'+myid+',3,'+data[3]+','+data[4]+','+data[5]
                sock.sendto(m,(ip,port))

            elif task=='0': #Ping 
                if j==True:       
                    if done==False:
                        print 'Send results uptil now'
                        files = currfile-1
                        numres = 0
                        for result in results:
                            if result[1]>files:
                                break
                            numres+=1
                        print files, numres
                        filesp = files
                        message = '15440,'+myid+',6,'+str(sf)+','+str(files)+','+data[5]            
                        rpo = 0
                        for result in results.copy():
                            rpo+=1
                            print 'rpo: ',rpo
                            filename = result[0].replace(',','')
                            
                            fn = ','+filename+','
                            message+=fn
                            
                            for one in results[result]:
                                message += '!!$^'+one

                            # message+=','
                            
                            if len(message)>=55000:
                                sock.sendto(message,(ip,port))
                                print 'Message Sent'
                                time.sleep(0.5)
                                message = '15440,'+myid+',6,'+str(sf)+','+str(files)+','+data[5]
                            
                            elif rpo-numres==0:
                                sock.sendto(message,(ip,port))
                                print 'Message Sent'
                                time.sleep(0.5)
                                message = '15440,'+myid+',6,'+str(sf)+','+str(files)+','+data[5]
                                break
                            results.pop(result)
                        if numres==0:
                            m = '15440,'+myid+',10,'+str(sf)+','+str(files)+',xx,x'
                            sock.sendto(m,addr)
                            print 'MS'
                            time.sleep(0.5)
                            
                        
                    elif done==True:
                        files = currfile-1
                        if len(results)==0:
                            print 'Done Not found'
                            print files
                            message = '15440,'+myid+',4,'+str(sf)+','+str(ef)+','+data[5]
                            sock.sendto(message,addr)
                            print 'Message Sent'
                            time.sleep(0.5)
                        else:
                            print 'Done found'
                            print files
                            message = '15440,'+myid+',6,'+str(sf)+','+str(ef)+','+data[5]
                            rp = 0
                            length = len(results)
                            for result in results.copy():
                                rp+=1
                                filename = result[0].replace(',','')
                                fn = ','+filename+','
                                message+=fn
                            
                                for one in results[result]:
                                    message += '!!$^'+one

                                # message+=','
                            
                                if len(message)>=55000:
                                    if rp-length==0:
                                        x = message.split(',')
                                        x[2]= '5'
                                        message = ','.join(x)
                                        sock.sendto(message,(ip,port))
                                        print 'Message Sent 1'
                                        time.sleep(0.5)
                                    else:
                                        sock.sendto(message,(ip,port))
                                        print 'Message Sent 2'
                                        time.sleep(0.5)
                                        message = '15440,'+myid+',6,'+str(sf)+','+str(files)+','+data[5]
                            
                                elif rp-length==0:
                                    x = message.split(',')
                                    x[2]= '5'
                                    message = ','.join(x)
                                    sock.sendto(message,(ip,port))
                                    print 'Message Sent 3'
                                    # time.sleep(0.5)
                                    message = '15440,'+myid+',5,'+str(sf)+','+str(files)+','+data[5]

                                results.pop(result)
                            
                            results.clear()
                        n=1
                        currfile = 0
                        done = False
                        j = False
                else:
                    m = '15440,'+myid+',10,xx,xx,xx,x'
                    sock.sendto(m,addr)
                    time.sleep(0.5)

            elif task=='7': #Job cancellation
                if j==True:
                    print "Work is being done \n"
                    terminate = True
                print 'Thread closed \n'
                j = False
                results.clear()
        data,addr = sock.recvfrom(61439)
        data = data.split(',')


   

if __name__ == "__main__":


    main(sys.argv)
