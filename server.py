import sys
import socket  
import timeit
import os
import Queue
import time
import pickle


class workerclient(object):
    def __init__(self,idd,addr):
        self.id = idd
        self.addr = addr
        self.pings = 0
        self.job = False
        self.ack = False
        self.jobid = '-1'
        self.jobtime = 0.0
        self.query = ''
        self.frange = ''
        self.Lastresults = ''
        self.Results = {}

    def reset(self):
        self.job = False
        self.ack = False
        self.jobid = '-1'
        self.jobtime = 0.0
        self.query = ''
        self.frange = ''
        self.Lastresults = ''
        self.Results = {}        

    def processresults(self,rdata):
        for result in rdata:
            if '!!$^' in result:
                rdata[rdata.index(result)] = result.split('!!$^')
        for k in rdata:
            rdata[rdata.index(k)] = filter(None,k)
        for i in xrange(0,len(rdata),2):
            try:
                self.Results[rdata[i]] = rdata[i+1]
            except:
                for k in range(0,i+1,2):
                    print rdata[k],':',rdata[k+1],'\n \n'
                # print rdata[i-2],':',rdata[i-1],'\n \n'
                raw_input()
                break

    def assignjob(self,jid,query,ran):
        self.job = True
        self.ack = False
        self.jobtime = time.clock()
        self.jobid = jid
        self.query = query
        self.frange = ran

class requestclient(object):
    """docstring for requestclient"""
    def __init__(self, idd,addr,query):
        self.id = idd
        self.addr = addr
        self.query = query
        self.sentchunks = []
        self.lastrange = 0
        self.Results = {}
        self.workers = [] #By ids
        self.donechunks = [] #Chunks for which results have been received
        self.numpings = 0
        self.lastpinged = 0.0

    def iscomplete(self):
        for chunk in self.sentchunks:
            if chunk not in self.donechunks:
                return False
        return True


        

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

def mainresults(rdata,jid):
    global Results
    for result in rdata:
        if '!!$^' in result:
            rdata[rdata.index(result)] = result.split('!!$^')
    for k in rdata:
        rdata[rdata.index(k)] = filter(None,k)
    for i in xrange(0,len(rdata),2):
        Results[jid][rdata[i]] = rdata[i+1]
        # print rdata[i],': ',rdata[i+1]
    # print len(Results)
    # raw_input()


workerspending = Queue.Queue(0) #All worker clients waiting for a job- workerclient obj
requestspending = Queue.Queue(0) #All request clients (jid,(srange,erange))

try:
    wp = pickle.load(open('workerspending.txt','rb'))
    reqp = pickle.load(open('requestspending.txt','rb'))
    for obj in wp:
        workerspending.put_nowait(obj)
    for obj in reqp:
        requestspending.put_nowait(obj)
except:
    pass


allfiles = paths()

try:
    jobs = pickle.load(open('jobs.txt','rb'))
except:
    jobs = {} #All jobs received from various clients, work under construction. Key: (clientid,addr), Value: [query,[All workers assigned to the job (list of tuples)],[Search data assigned to each worker(tuples)]]


try:
    jobscancelled = pickle.load(open('jobscancelled.txt','rb'))
except:
    jobscancelled = {} #Jobs that are in the queue but cancelled (id,addr)

try:
    workers = pickle.load(open('workers.txt','rb'))
except:
    workers = {} #Workers doing or willing to do a job workerclient obj

try:
    deadworkers = pickle.load(open('deadworkers.txt','rb'))
except:
    deadworkers = {}


    

## ['Man',[(1,('localhost',3547)),(2,('localhost',5855))],[(xxxx,yyyy),(aaaa,bbbb)]]

try:
    Results = pickle.load(open('results.txt','rb'))
except:
    Results = {} #Dictionary with key: (cid,addr) and value another dict(key: filename, value: array of lines)


try:
    c = pickle.load(open('misc.txt','rb'))
    wclient = c[0]
    rclient = c[1]
except:
    wclient = 0
    rclient = 100

def main(argv):
    global workerspending
    global requestspending
    global allfiles
    global jobs
    global jobscancelled
    global workers
    global deadworkers
    global Results
    global wclient
    global rclient

    ip=str(argv[1])
    port=int(argv[2]) 

    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.setblocking(0)
    sock.bind((ip,port))
    # t0 = time.clock()
    t0 = time.clock()
    while 1:
        print 'Alive: ',len(workers)
        print 'tasks: ',requestspending.qsize()
        try:
            
            data,addr = sock.recvfrom(61439)
            datac = data.split(',')
            # print datac
            
            if datac[0]=='15440':

                if datac[2]=='0': #Ping from request client
                    jobs[datac[1]].numpings+=1
                    jobs[datac[1]].lastpinged = time.clock()
                    if jobs[datac[1]].iscomplete():
                        files = len(Results[datac[1]])
                        if files==0:
                            message = '15440,'+datac[1]+',4,'+'0'+','+str(files)+','+data[5]
                            sock.sendto(message,addr)
                        else:    
                            message = '15440,'+datac[1]+',6,'+'0'+','+str(files)+','+data[5]  
                            rp = 0          
                            for result in Results[datac[1]].copy():
                                rp+=1
                                filename = result
                                fn = ','+filename+','
                                message+=fn
                                for one in Results[datac[1]][result]:
                                    message += '!!$^'+one
                                    # Results[datac[1]][result].remove(one)   

                                # message+=','
                                if len(message)>=55000:
                                    if rp-files==0:
                                        x = message.split(',')
                                        x[2] = '5'
                                        message = ','.join(x)
                                        sock.sendto(message,addr)
                                        print 'Message Sent 1'
                                    else:
                                        sock.sendto(message,addr)
                                        print 'Message Sent 1'
                                        message = '15440,'+datac[1]+',6,'+data[3]+','+str(files)+','+data[5]
                                elif rp-files==0:
                                    sock.sendto(message,addr)
                                    print 'Message Sent 1'
                                    message = '15440,'+datac[1]+',5,'+data[3]+','+str(files)+','+data[5]
                                    break
                                Results[datac[1]].pop(result)
                            Results[datac[1]].clear()

                    else:
                        files = len(Results[datac[1]])-1
                        message = '15440,'+datac[1]+',6,'+'0'+','+str(files)+','+data[5] 
                        rp = 0           
                        for result in Results[datac[1]].copy():
                            rp+=1
                            filename = result
                            fn = ','+filename+','
                            message+=fn
                            for one in Results[datac[1]][result]:
                                message += '!!$^'+one
                                # Results[datac[1]][result].remove(one)
                            # message+=','
                            if len(message)>=55000:
                                sock.sendto(message,addr)
                                print 'Message Sent'
                                message = '15440,'+datac[1]+',6,'+data[3]+','+str(files)+','+data[5]
                            if rp-files==0:
                                sock.sendto(message,addr)
                                print 'Message Sent'
                                message = '15440,'+datac[1]+',6,'+data[3]+','+str(files)+','+data[5]
                                break
                            Results[datac[1]].pop(result)

                
                if datac[2]=='1': ##Request to join
                    wclient+=1
                    c_id = str(wclient)
                    a = workerclient(c_id,addr)
                    workers[c_id] = a
                    print workers[c_id].addr, ' has been added to workers' 
                    workerspending.put_nowait(workers[c_id])

                elif datac[2]=='3': ##Job-ACK
                    workers[datac[1]].ack = True



                elif datac[2]=='4': ##No results found
                    workers[datac[1]].pings-=1
                    if workers[datac[1]].pings<0:
                        workers[datac[1]].pings = 0
                    ind = jobs[workers[datac[1]].jobid].workers.index(datac[1])
                    jobs[workers[datac[1]].jobid].workers.pop(ind)
                    jobs[workers[datac[1]].jobid].donechunks.append(workers[datac[1]].frange)
                    
                    print addr,' did not find any results '
                    # print 'Sent chunks: ',jobs[workers[datac[1]].jobid].sentchunks
                    # print 'Done chunks: ',jobs[workers[datac[1]].jobid].donechunks
                    raw_input()
                    workers[datac[1]].reset()
                    workerspending.put_nowait(workers[datac[1]])
                elif datac[2]=='5': ##Done Found results
                    workers[datac[1]].pings-=1
                    if workers[datac[1]].pings<0:
                        workers[datac[1]].pings = 0
                    workers[datac[1]].processresults(datac[6:])
                    
                    if workers[datac[1]].jobid not in Results.copy().keys():
                        Results[workers[datac[1]].jobid] = {} 
                    mainresults(datac[6:],workers[datac[1]].jobid)
                    
                    ind = jobs[workers[datac[1]].jobid].workers.index(datac[1])
                    jobs[workers[datac[1]].jobid].workers.pop(ind)
                    jobs[workers[datac[1]].jobid].donechunks.append(workers[datac[1]].frange)
                    
                    print 'found results'

                    # print 'Sent chunks: ',jobs[workers[datac[1]].jobid].sentchunks
                    # print 'Done chunks: ',jobs[workers[datac[1]].jobid].donechunks
                    
                    workers[datac[1]].reset()
                    workerspending.put_nowait(workers[datac[1]])
                    raw_input()
                elif datac[2]=='6': ##Job not completed
                    # workers[datac[1]].pings-=1
                    workers[datac[1]].processresults(datac[6:])
                    
                    if workers[datac[1]].jobid not in Results.copy().keys():
                        Results[workers[datac[1]].jobid] = {} 
                    mainresults(datac[6:],workers[datac[1]].jobid)
                    
                    workers[datac[1]].pings-=1
                    if workers[datac[1]].pings<0:
                        workers[datac[1]].pings = 0
                    workers[datac[1]].Lastresults = datac[4]
                    print 'Not completed'
                
                elif datac[2]=='7': ##Cancel job
                    j_id = datac[1]
                    job = jobs.pop(j_id,0)
                    jobscancelled[j_id] = job
                    for worker in job.workers:
                        m = '15440,'+str(worker)+',7,xx,xx,xx,'+job.query
                        sock.sendto(m,workers[worker].addr) ##Make a general send message function that handles timeouts and shit
                        workers[worker].reset()
                        workerspending.put_nowait(workers[worker])
                    job.workers[:] = []
                    Results.pop(datac[1])
                    print 'Job cancelled'
                
                elif datac[2]=='8': ##Query/Add request client Message format: 15440,id,8,query
                    rclient+=1
                    datac[1] = str(rclient)
                    a = requestclient(datac[1],addr,datac[3])
                    jobs[datac[1]] = a 
                    x = 0
                    if len(jobs)>1:
                        temp = Queue.Queue(0)
                        temp1 = Queue.Queue(0)
                        for i in range(0,requestspending.qsize()):
                            temp.put_nowait(requestspending.get_nowait())
                        while (len(allfiles)-x)>10000: #Break the job into chunks and put in the queue
                            temp1.put_nowait((datac[1],(x,x+10000)))
                            x+=10000
                        temp1.put_nowait((datac[1],(x,len(allfiles))))

                        l = temp1.qsize()+temp.qsize()
                        for k in range(0,l):
                            if k%2==0:
                                if not temp1.empty():
                                    requestspending.put_nowait(temp1.get_nowait())
                                else:
                                    requestspending.put_nowait(temp.get_nowait())    
                            else:
                                if not temp.empty():
                                    requestspending.put_nowait(temp.get_nowait())
                                else:
                                    requestspending.put_nowait(temp1.get_nowait())
                    else:
                        while (len(allfiles)-x)>10000: #Break the job into chunks and put in the queue
                            requestspending.put_nowait((datac[1],(x,x+10000)))
                            x+=10000
                        requestspending.put_nowait((datac[1],(x,len(allfiles))))


                    m = '15440,'+datac[1]+',2,'+datac[3]
                    sock.sendto(m,addr)
                    Results[datac[1]] = {}
                
                elif datac[2]=='10': # Ping response from workers who are not doing a job
                    workers[datac[1]].pings-=1
                    if workers[datac[1]].pings<0:
                        workers[datac[1]].pings = 0
        
        except socket.error as error:
            print "Waiting for message from clients"
            time.sleep(0.25)


        for job in jobs.copy():
            t = time.clock()
            print 'Job: ',job,'Time since ping ',t-jobs[job].lastpinged,'\n'
            if t-jobs[job].lastpinged>7:
                for worker in jobs[job].workers:
                    m = '15440,'+str(worker)+',7,xx,xx,xx,'+jobs[job].query
                    sock.sendto(m,workers[worker].addr) ##Make a general send message function that handles timeouts and shit
                    workers[worker].reset()
                    workerspending.put_nowait(workers[worker])
                jobs[job].workers[:] = []
                j = jobs.pop(job,0)
                jobscancelled[j.id] = j
                if j.id in Results.keys():
                    Results.pop(j.id)
                print 'Job cancelled'            



        if bool(jobs): 
            t = time.clock()
            for job in jobs:
                for worker in jobs[job].workers:
                    if t - workers[worker].jobtime >3:
                        if workers[worker].ack==False:
                            if workerspending.empty():
                                requestspending.put_nowait((workers[worker].jobid,workers[worker].frange))
                                jobs[workers[worker].jobid].workers.remove(worker)
                                ind = jobs[workers[worker].jobid].sentchunks.index(workers[worker].frange)
                                jobs[workers[worker].jobid].sentchunks.pop(ind)
                                workers[worker].reset()
                                deadworkers[worker] = workers[worker]
                                workers.pop(worker,0)
                            else: #Assign job to new worker. Put other back in queue
                                newworker = workerspending.get_nowait()
                                newworker.assignjob(workers[worker].jobid,workers[worker].query,workers[worker].frange)
                                message = '15440,'+str(newworker.id)+',2,'+str(newworker.frange[0])+','+str(newworker.frange[1])+','+newworker.query
                                sock.sendto(message,newworker.addr)
                                jobs[workers[worker].jobid].workers.remove(worker)
                                jobs[newworker.jobid].workers.push(newworker)
                                workers[worker].reset()
                                deadworkers[worker] = workers[worker]
                                workers.pop(worker,0)
                            print worker,'did not send an ack and has been removed'
                            # p = workers[worker].pings
                            # workers[worker].reset()
                            # workers[worker].pings = p

        if not workerspending.empty():
            if not requestspending.empty():
                worker = workerspending.get_nowait()
                if str(worker.id) not in deadworkers.copy().keys():
                    request = requestspending.get_nowait() #(id,(srange,erange))
                    if request[0] not in jobscancelled.copy().keys():
                        worker.assignjob(request[0],jobs[request[0]].query,request[1])
                        message = '15440,'+str(worker.id)+',2,'+str(request[1][0])+','+str(request[1][1])+','+jobs[request[0]].query
                        sock.sendto(message,worker.addr)
                        print 'Job assigned'
                        jobs[request[0]].workers.append(str(worker.id))
                        jobs[request[0]].sentchunks.append(request[1])

        t1 = time.clock()
        print t1-t0
        if t1-t0>0.15:
            for worker in workers.copy():
                if workers[worker].pings<3:
                    m = '15440,'+str(worker)+',0,xx,xx,xx,x'
                    sock.sendto(m,workers[worker].addr)
                    workers[worker].pings+=1
                    print 'Pinged',workers[worker].addr,' ',workers[worker].pings,' times'
                else:
                    deadworkers[worker] = workers[worker]
                    if workers[worker].jobid!='-1' and workers[worker].jobid not in jobscancelled.copy().keys():
                        if workerspending.empty():
                            ind = jobs[workers[worker].jobid].workers.index(workers[worker].id)
                            jobs[workers[worker].jobid].workers.pop(ind)
                            jobs[workers[worker].jobid].sentchunks.remove(workers[worker].frange)
                            r1 = workers[worker].frange[0]
                            r2 = workers[worker].frange[1]
                            lr = int(workers[worker].Lastresults)
                            jobs[workers[worker].jobid].sentchunks.append((r1,lr))
                            jobs[workers[worker].jobid].donechunks.append((r1,lr))
                            requestspending.put_nowait((workers[worker].jobid,(lr,r2)))
                        else:
                            ind = jobs[workers[worker].jobid].workers.index(workers[worker].id)
                            jobs[workers[worker].jobid].workers.pop(ind)
                            jobs[workers[worker].jobid].sentchunks.remove(workers[worker].frange)
                            r1 = workers[worker].frange[0]
                            r2 = workers[worker].frange[1]
                            lr = int(workers[worker].Lastresults)
                            jobs[workers[worker].jobid].sentchunks.append((r1,lr))
                            jobs[workers[worker].jobid].donechunks.append((r1,lr))
                            newworker = workerspending.get_nowait()
                            newworker.assignjob(workers[worker].jobid,workers[worker].query,(lr,r2))
                            message = '15440,'+str(newworker.id)+',2,'+str(newworker.frange[0])+','+str(newworker.frange[1])+','+newworker.query
                            sock.sendto(message,newworker.addr)
                            jobs[newworker.jobid].workers.append(newworker)
                            jobs[newworker.jobid].sentchunks.append((lr,r2))      
                    workers.pop(worker,0)
            t0 = time.clock()
        else:
            time.sleep(0.5)

        pickle.dump(jobs,open('jobs.txt','wb'))
        pickle.dump(jobscancelled,open('jobscancelled.txt','wb'))
        pickle.dump(workers,open('workers.txt','wb'))
        pickle.dump(deadworkers,open('deadworkers.txt','wb'))
        pickle.dump(Results,open('results.txt','wb'))
        reqp = list(requestspending.queue)
        pickle.dump(reqp,open('requestspending.txt','wb'))
        wp = list(workerspending.queue)
        pickle.dump(wp,open('workerspending.txt','wb'))
        d = [wclient,rclient]
        pickle.dump(d,open('misc.txt','wb'))


if __name__ == "__main__":
    main(sys.argv)


