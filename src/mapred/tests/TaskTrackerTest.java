package mapred.tests;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import mapred.interfaces.Mapper;
import mapred.interfaces.Reducer;
import mapred.messages.ClientAPIMsg;
import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;
import mapred.types.MapReduceJob;

/*
 * This is a test bench to test our TaskTracker, behaves like our 
 * JobTracker.
 */

public class TaskTrackerTest {
	/***********************************************/
	public static void main(String[] args){
		ServerSocket TTsocket = null;
		Socket requestSocket = null;
		ObjectOutputStream requestStream = null;
		Mapper map = new DefaultMapper();
		Reducer reduce = new DefaultReducer();
		
		// Open up communications 
				try {		
					// open a new server socket for response from TaskTracker
					TTsocket = new ServerSocket(10000);
					// a client socket for sending messages
					// to a TaskTracker on the same machine
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// Test 1. Send a sequence of 15 empty "map" requests to TaskTracker
				/*try {
					
					for(int i=0; i<1;i++){
						MapReduceJob job = new MapReduceJob();
						job.setIpFileName("Dummy.txt");
						job.setJobName("Distributed Dummy");
						job.setJobId(100+i);
						job.setMapper(map);
						job.setReducer(reduce);
						job.setNumReducers(2);
						requestSocket = new Socket(InetAddress.getLocalHost(), 10001);
						requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
						MasterToSlaveMsg launchReq = new MasterToSlaveMsg();
						launchReq.setMsgType("start");
						launchReq.setJob(job);
						launchReq.setTaskType("map");
						List<String> ipFiles = new ArrayList<String>();
						ipFiles.add(job.getIpFileName());
						launchReq.setIpFiles(ipFiles);
						launchReq.setReadRecordStart(10*i);
						launchReq.setReadRecordEnd(10*i + 9);
						launchReq.setTaskId(i);
						requestStream.writeObject(launchReq);
						requestStream.close();
						requestSocket.close();
						System.out.println("Sent a map task request");
						
						// Wait for reply response
						Socket TT = TTsocket.accept();
						ObjectInputStream TTStream = new ObjectInputStream(TT.getInputStream());
						SlaveToMasterMsg reply = (SlaveToMasterMsg) TTStream.readObject();
						String response = reply.getType();
						TTStream.close();
						TT.close();
						System.out.println("TaskTracker response is: " + response);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				// Test 2. Send a sequence of 3 "kill" requests to TaskTracker
				/*try {
					
					for(int i=0; i<3;i++){
						requestSocket = new Socket(InetAddress.getLocalHost(), 10001);
						requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
						MasterToSlaveMsg stopReq = new MasterToSlaveMsg();
						stopReq.setMsgType("stop");
						stopReq.setJobStopId(104+i);
						stopReq.setTaskType("map");
						List<String> ipFiles = new ArrayList<String>();
						//stopReq.setTaskId(i);
						requestStream.writeObject(stopReq);
						requestStream.close();
						requestSocket.close();
						System.out.println("Sent a stop job request");
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				// Test 3. Send a sequence of 4 empty "map" requests to TaskTracker
				/*try {
					
					for(int i=0; i<4;i++){
						MapReduceJob job = new MapReduceJob();
						job.setIpFileName("Dummy.txt");
						job.setJobName("Distributed Dummy");
						job.setJobId(116+i);
						job.setMapper(map);
						job.setReducer(reduce);
						requestSocket = new Socket(InetAddress.getLocalHost(), 10001);
						requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
						MasterToSlaveMsg launchReq = new MasterToSlaveMsg();
						launchReq.setMsgType("start");
						launchReq.setJob(job);
						launchReq.setTaskType("map");
						List<String> ipFiles = new ArrayList<String>();
						ipFiles.add(job.getIpFileName());
						launchReq.setIpFiles(ipFiles);
						launchReq.setReadRecordStart(10*i);
						launchReq.setReadRecordEnd(10*i + 9);
						launchReq.setTaskId(i);
						requestStream.writeObject(launchReq);
						requestStream.close();
						requestSocket.close();
						System.out.println("Sent a map task request");
						
						// Wait for reply response
						Socket TT = TTsocket.accept();
						ObjectInputStream TTStream = new ObjectInputStream(TT.getInputStream());
						SlaveToMasterMsg reply = (SlaveToMasterMsg) TTStream.readObject();
						String response = reply.getType();
						TTStream.close();
						TT.close();
						System.out.println("TaskTracker response is: " + response);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				
				// Test 4. Send an empty "reduce" request to TaskTracker
				try {
					
					for(int i=0; i<1;i++){
						MapReduceJob job = new MapReduceJob();
						job.setIpFileName("Dummy.txt");
						job.setJobName("Distributed Dummy");
						job.setJobId(100+i);
						job.setMapper(map);
						job.setReducer(reduce);
						job.setNumReducers(1);
						requestSocket = new Socket(InetAddress.getLocalHost(), 10001);
						requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
						MasterToSlaveMsg launchReq = new MasterToSlaveMsg();
						launchReq.setMsgType("start");
						launchReq.setJob(job);
						launchReq.setTaskType("reduce");
						List<String> ipFiles = new ArrayList<String>();
						ipFiles.add(job.getIpFileName());
						launchReq.setIpFiles(ipFiles);
						launchReq.setTaskId(i);
						requestStream.writeObject(launchReq);
						requestStream.close();
						requestSocket.close();
						System.out.println("Sent a reduce task request");
						
						// Wait for reply response
						Socket TT = TTsocket.accept();
						ObjectInputStream TTStream = new ObjectInputStream(TT.getInputStream());
						SlaveToMasterMsg reply = (SlaveToMasterMsg) TTStream.readObject();
						String response = reply.getType();
						TTStream.close();
						TT.close();
						System.out.println("TaskTracker response is: " + response);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		
	}

}
