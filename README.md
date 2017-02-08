# Simplified Distributed Streaming Platform
Client, server and platform communication.

### CURRENT BUILD
The current functionality only includes running a master server as platform. Where it will automatically get the host IP and available port number. When client runs the program they will need to enter the selected port number from the platform and it will create a socket connection. Then the client will select a name and pass any other data to platform.

<br>

Platform side:

<img src="https://github.com/Dorahan/Simplified_Distributed_Streaming_Platform/blob/master/master.gif" width="500">

Client side:

<img src="https://github.com/Dorahan/Simplified_Distributed_Streaming_Platform/blob/master/client.gif" width="500">

****

### PROJECT SCOPE
**Platform**  
Functions that the platform can do:

1. Create / Delete topic with partitions
2. Subscribe / Unsubscribe
  1. Assign available partition to client
3. Publish / Get topic
  1. Transfer msg to a server by doing MD5SUM
4. Add and Delete server from platform

**Client**  
Functions that the client can do:

1. Create / Delete topic with partitions
2. Subscribe / Unsubscribe to Topic (and partition)
3. Publish / Get topic
4. Add and Delete server from platform


**Server**  
Functions that the server can do:

1. Store (info and data)
2. Extract
