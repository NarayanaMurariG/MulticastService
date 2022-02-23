#include<stdio.h>
#include<unistd.h>
#include<iostream>
#include<sstream>
#include<arpa/inet.h>
#include<netinet/in.h>
#include<stdlib.h>
#include<time.h>
#include<pthread.h>
#include<string>
#include<strings.h>
#include<queue>
#include<bits/stdc++.h>
#include<cstdlib>
#include<signal.h>
#include<queue>
using namespace std;

void *multicastSender(void *args);
void *multicastReceiver(void *args);
void *deliveredBufferedMessages(void *args);
void sendMessageToProcess(int processNumber,int clock[]);
void processTheMessage(int msgReceivedFrom,int clientClock[],int arraySize);
string printVectorTimeStamp(int vC[]);
void signalHandler(int signalNumber);
void *terminateProgram(void *args);
void *sendMulticastMessages(void *args);

/*
This data structure is to store buffered messages
and the process from which it is received
*/
struct Message{
  int clock[3];
  int receivedFrom;
};

/*
This data structure is to store messages to be delivered
and the process to which it has to be delivered.

To simulate the channel delay(to show buffering), I have included a flag shouldDelay
which is randomly set to true for some messages, which will be delivered
delivered lately.
*/
struct DeliveryMessage{
  bool shouldDelay;
  int processNumber;
  int vectorClock[3];
};


const int BUFFER_SIZE = 200;
const int MAX_LISTEN_QUEUE_SIZE = 100;
const int TOTAL_NUMBER_OF_PROCESSES = 3;
//Predefining the number of multicast messages to 4.
const int numberOfMulticasts = 4;
int vectorClock[3] = {0,0,0};
int portNumbers[3] = {8070,8080,8090};
int processNumber = 0;
int *socketFdAddress;
queue<Message> bufferQueue;
queue<DeliveryMessage> deliveryQueue;
bool scanBufferQueue = false;


int main(int argc,char* argv[]){
  if(argc < 1){
    printf("Please enter the process number and restart again\n");
    exit(1);
  }

  processNumber = atoi(argv[1]);
  pthread_t senderThread, receiverThread, terminateThread, bufferDelivery, messageDelivery;


  //Socket establishment for listening multicast messages.
  struct sockaddr_in serverAddress;
	int addresslen = sizeof(serverAddress);
	int socketfd = socket(AF_INET, SOCK_STREAM, 0);
  socketFdAddress = &socketfd;
	//binding mediator `to socket
	serverAddress.sin_family = AF_INET;
	serverAddress.sin_port = htons(portNumbers[processNumber-1]);
	serverAddress.sin_addr.s_addr = INADDR_ANY;

	//Binding socket to the port no defined in PORT
	if( bind(socketfd, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) == 0){ // returns -1 if failed
		printf("Binded Socket Successfully to the port number\n");
	} else{
		printf("Binding Socket Failed...\n Try After 10-15s \n");
		printf("Sometimes this happens when we stop server and start it immediately\n");
		exit(0);
	}

  if(listen(socketfd,MAX_LISTEN_QUEUE_SIZE) < 0){
    printf("Listener failed, try again");
    exit(1);
  }


  pthread_create(&senderThread,NULL,&multicastReceiver,&socketfd);
  pthread_create(&terminateThread,NULL,&terminateProgram,NULL);
  // Adding sleep so that all the processes will have their receivers running
  sleep(5);
  pthread_create(&receiverThread,NULL,&multicastSender,NULL);
  pthread_create(&bufferDelivery,NULL,&deliveredBufferedMessages,NULL);
  pthread_create(&messageDelivery,NULL,&sendMulticastMessages,NULL);

  pthread_join(senderThread,NULL);
  pthread_join(receiverThread,NULL);
  pthread_join(terminateThread,NULL);
  pthread_join(bufferDelivery,NULL);
  pthread_join(messageDelivery,NULL);
  return 0;
}

//Receiver thread for incoming multicasts
void *multicastReceiver(void *args){
  int socketFd = *(int *)args;
	char buffer[BUFFER_SIZE] = {0};

  int newClientSocket,p1Clock,p2Clock,p3Clock,msgReceivedFrom;
  string p1ClockStr;
  string p2ClockStr;
  string p3ClockStr;
  struct sockaddr_in newClientAddress;
  int addresslen = sizeof(newClientAddress);
  while(true){
    if((newClientSocket = accept(socketFd,(struct sockaddr *)&newClientAddress,(socklen_t*)&addresslen)) < 0){
  	  printf("Error while accepting connection");
      exit(0);
    }
    else{
    		read(newClientSocket,buffer,BUFFER_SIZE);
        // printf("Message Received : %s\n",buffer);
        string msgReceivedFromStr = strtok(buffer,",");
        string p1ClockStr = strtok(NULL,",");
  			string p2ClockStr = strtok(NULL,",");
  			string p3ClockStr = strtok(NULL,",");

  			//Converting strings to integers
        msgReceivedFrom = std::stoi(msgReceivedFromStr);
  			p1Clock = std::stoi(p1ClockStr);
  			p2Clock = std::stoi(p2ClockStr);
        p3Clock = std::stoi(p3ClockStr);
        int receivedVectorClock[3] = {p1Clock,p2Clock,p3Clock};

        processTheMessage(msgReceivedFrom,receivedVectorClock,3);
        bzero(buffer,BUFFER_SIZE);
        string token = "Received";
        send(newClientSocket,token.c_str(),token.size(),0);
        close(newClientSocket);
      }
  }

  return NULL;
}

//Thread to send multicast messages to peers
void *multicastSender(void *args){

  for(int k =0 ;k<numberOfMulticasts;k++){
    sleep(2);
    vectorClock[processNumber-1]++;
    cout << "Multicasted following Message over the Network : " << printVectorTimeStamp(vectorClock) << "\n";
    int randomNumber;
    for(int i=1;i<=3;i++){
      if(i != processNumber){
        //some messages will have delay which is assigned randomly
        randomNumber = rand() % 2;
        struct DeliveryMessage msg;
        if(randomNumber == 0){
          msg.shouldDelay = true;
        }else{
          msg.shouldDelay = false;
        }
        msg.processNumber = i;
        msg.vectorClock[0] = vectorClock[0];
        msg.vectorClock[1] = vectorClock[1];
        msg.vectorClock[2] = vectorClock[2];
        // pushing message to delivery queue
        deliveryQueue.push(msg);
      }
    }
  }
  return NULL;

}

//Sending message to individual process by connecting to that process
void sendMessageToProcess(int processNumberToSend,int clock[]){
  int socketfd;
  char buffer[BUFFER_SIZE] = {0};
  struct sockaddr_in serverAddress;
  int addresslen = sizeof(serverAddress);
  socketfd = socket(AF_INET,SOCK_STREAM,0);
  int portNumber = 0;
  // cout << "Sending message to " << processNumberToSend << "\n";
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(portNumbers[processNumberToSend-1]);
  inet_pton(AF_INET,"127.0.0.1",&serverAddress.sin_addr);
  connect(socketfd,(struct sockaddr *)&serverAddress,sizeof(serverAddress));

  std::ostringstream stream;
  stream << processNumber << ",";
  for (int i=0;i<3;i++) {
      stream << clock[i] << ",";
  }

  std::string token(stream.str());
  send(socketfd,token.c_str(),token.size(),0);

  read(socketfd,buffer,BUFFER_SIZE);
  bzero(buffer,BUFFER_SIZE);
  close(socketfd);
}

/*
This method contains the logic to implement the process delivery to the
application. It uses the rules for causal ordered multicast and delivers only
if conditions are satisfied. Or else it buffers until conditions are met.
*/
void processTheMessage(int msgReceivedFrom,int clientClock[],int arraySize){

  //Update the clock and print the timestamp.
  int beforeClock[3];
  for(int i=0;i<arraySize;i++){
        beforeClock[i] = vectorClock[i];
  }

  bool cond1 = false;
  bool cond2 = true;

    if(vectorClock[msgReceivedFrom-1] + 1 == clientClock[msgReceivedFrom-1]){
        cond1 = true;
    }else{
      for(int i=0;i<arraySize;i++){
        if(i != msgReceivedFrom-1){
          if(clientClock[i] > vectorClock[i]){
            cond2 = false;
            break;
          }
        }
      }
    }

    if(cond1 && cond2){
    //Delivering the message to application
    vectorClock[msgReceivedFrom-1] = clientClock[msgReceivedFrom-1];
      cout << "Msg Received From Process : " << msgReceivedFrom << ", Client Clock : " << printVectorTimeStamp(clientClock) << ", Before : "<< printVectorTimeStamp(beforeClock) << ", After : "<< printVectorTimeStamp(vectorClock) << "\n";
    }else{
      cout << "Msg Received From Process : " << msgReceivedFrom << ", Client Clock : " << printVectorTimeStamp(clientClock) <<  ", MyClock : "<< printVectorTimeStamp(vectorClock) << "\n";
      cout << "Buffering the message \n";
      struct Message msg;
      msg.receivedFrom = msgReceivedFrom;
      msg.clock[0] = clientClock[0];
      msg.clock[1] = clientClock[1];
      msg.clock[2] = clientClock[2];
      bufferQueue.push(msg);
      //Buffer the message by adding to bufferQueue.
    }
}

/*
This thread sends messages added to delivery Queue
if a message has to be delayed it will be dequeued, flag will be toggled
and the added at the back of the queue.
*/
void *sendMulticastMessages(void *args){
  while(true){

    while (!deliveryQueue.empty()) {
      struct DeliveryMessage msg = deliveryQueue.front();
      if(msg.shouldDelay){
        deliveryQueue.pop();
        msg.shouldDelay = false;
        deliveryQueue.push(msg);
      }else{
        sendMessageToProcess(msg.processNumber,msg.vectorClock);
        deliveryQueue.pop();
      }
    }
    sleep(5);
  }
  return NULL;
}

/*
This thread keeps on running to check if any buffered messages can be
delivered to the application. While iterating through the queue, it delivers
the messages which satisfies the causal ordered conditions
*/
void *deliveredBufferedMessages(void *args){

    //When This Method is called scan through queue and deliver the messages
    //Deliver messages which satisfy the conditions
  while(true){
    while (!bufferQueue.empty()) {
      int beforeClock[3];
      for(int i=0;i<3;i++){
            beforeClock[i] = vectorClock[i];
      }
      bool cond1 = false;
      bool cond2 = true;
      struct Message msg = bufferQueue.front();
      if(vectorClock[msg.receivedFrom-1] + 1 == msg.clock[msg.receivedFrom-1]){
          cond1 = true;
      }else{
        for(int i=0;i<3;i++){
          if(i != msg.receivedFrom-1){
            if(msg.clock[i] > vectorClock[i]){
              cond2 = false;
              break;
            }
          }
        }
      }

      if(cond1 && cond2){
        //Deliver the message
        vectorClock[msg.receivedFrom-1] = msg.clock[msg.receivedFrom-1];
        cout << "Msg Received From Process : " << msg.receivedFrom << ", Client Clock : " << printVectorTimeStamp(msg.clock) << ", Before : "<< printVectorTimeStamp(beforeClock) << ", After : "<< printVectorTimeStamp(vectorClock) << "\n";
        bufferQueue.pop();
      }else{
        //Dequeue and enqueue the message
        bufferQueue.pop();
        bufferQueue.push(msg);
      }

    }
    sleep(1);
  }
  return NULL;
}

string printVectorTimeStamp(int vC[]){
  string output = "[";
  // std::cout << "[";
  for(int i=0;i<3;i++){
    if(i == 2){
      output.append(std::to_string(vC[i]));
    }
    else{
      output.append(std::to_string(vC[i]));
      output.append(",");
    }
  }
  output.append("]");
  return output;
}

void signalHandler(int signalNumber){
  cout << "\n Terminating the program \n" << endl;
  //closeSocketDescriptor
  if(close(*socketFdAddress) != 0){
    printf("Socket could not be closed properly\n");
  }else{
    printf("Socket closed Successfully\n");
  }
  exit(signalNumber);
}
void *terminateProgram(void *args){
  signal(SIGINT, signalHandler);
  return NULL;
}
