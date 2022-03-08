# Distributed Search Engine

## Description
Uses the "Partition-Aggregation" structure to implement a distributed search engine. Search query lookup is parallelized by assigning different workers to look in different parts of the database. This follows a client/server architecture where jobs distribution and results aggregation is handled by the server. Additionally, the server also receives query requests and sends the relevant search results.

## System Design
The system consists of three programs:

1. Server 
2. Worker Client
3. Request Client

### Server
* Recognizes the different types of clients.
* Keep track of current request clients and their queries.
* Keep track of available worker clients and their job assignments.
* Load balance between workers.
* Respond to pings from the request client.
* Ensure active status of the worker clients.
* Pings worker clients every 5s to ensure they are still online.

### Worker Client
* Multi-threaded client with one thread dedicated to hearing and responding control messages from the server.
* Searches for a given query string in the files assigned to it.
* Sends results to server as a ping response.

### Request Client
* Sends request query to the server.
* Pings server every 5s to ensure that request has not timed out.

### Messages
1. **PING (Code: 0)** - Request client pings server, server pings worker clients
2. **REQUEST\_TO_JOIN (Code: 1)** - Sent by worker client to become part of the network, received by server.
3. **JOB (Code: 2)** - Sent by server to assign job, received by the worker client.
4. **ACK_JOB (Code: 3)** - Sent by worker client to acknowledge job, received by the server.
5. **DONE\_NOT_FOUND (Code: 4)** - Worker client completes its job, no results found in the files assigned to it.
6. **DONE_FOUND (Code: 5)** - Job completed and results found
7. **NOT_DONE (Code: 6)** - Sent as response to ping when worker client is still processing the job assigned
8. **CANCEL_JOB (Code: 7)** - Informs that Request client has cancelled job
9. **QUERY (Code: 8)** - Request client wishes to make a query

### Failure Handling
* If a worker client times out three times (i.e. no ACK_JOB received), server assigns job to the next available worker.
* Worker client sends status results (DONE\_NOT\_FOUND, DONE\_FOUND, NOT_DONE) as a response to server's pings. If 3 pings go unresponded, server reassigns the job.
* If server does not receive a ping from request client for 7s, it aborts the search request.
* If server fails, all state is saved to files. Upon restart, server is able to resume operations without any problems.

## Usage

### Prerequisites
* Linux
* Python 2.x

### Running the Application
Project directory must contain 1 server, at least 1 request client and worker client each. Additionally, it must contain a `database/` folder. This contains all subdirectories that need to be searched for the query match.

To run the server:
``` python server.py <ip> <port> ```

To run the worker client:
``` python workerclient.py <ip> <port>```

To run the request client:
``` python requestclient.py <ip> <port>```
All queries are provided as input at runtime.

## General Info
This application was developed for an in-class project on Network-Centric Computing (Spring 2017) at Lahore University of Management Sciences, Lahore, Pakistan


