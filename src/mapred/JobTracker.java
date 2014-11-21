package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import mapred.messages.ClientAPIMsg;
import mapred.types.JobTableEntry;
import mapred.types.Pair;

/*
 * Single object of this class runs on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker{
	
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from clientAPI
	private static ServerSocket clientAPISocket;
	// id of last launched job
	private static int lastJobId;
	// map of cluster nodes read from config file, each has a pair as value
	// first element of pair is status (up/down), second element is load (Integer)
	private static ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	// The IP Addr of the namenode, read from config file
	private static String nameNode;

	
	public static void main(String[] args) {
		
		try {
			// TODO : Initialize list of clusters from config file
			clusterNodes = new ConcurrentHashMap<String, Pair<String, Integer>>();
			// for testing, just add this node to the list
			Pair<String, Integer> temp = new Pair<String, Integer>();
			temp.setFirst("up");
			temp.setSecond(0);
			clusterNodes.put(InetAddress.getLocalHost().getHostAddress(), temp);
			// read namenode from config file
			// for testing, just let this node be namenode
			nameNode = InetAddress.getLocalHost().getHostAddress();
			
			// Do various init routines
			// initialise empty jobs list
			mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
			
			lastJobId = 0;
			
			// initialise clientAPI socket
			clientAPISocket = new ServerSocket(20000);
			
			// start the jobtracker monitoring thread
			JTMonitor jtm = new JTMonitor(mapredJobs, clusterNodes);
			Thread monitorThread = new Thread(jtm);
			monitorThread.start();
			
			// start the jobtracker dispatcher thread
			Thread dispatcherThread = new Thread(new JTDispatcher(mapredJobs, clusterNodes, nameNode));
			dispatcherThread.start();
			
		} catch (IOException e) {
			System.out.println("JobTracker can't detect cluster machines");
		}
		
		// Start listening for mapreduce jobs from clientAPI
		while(true){
			try {
				Socket client = clientAPISocket.accept();
				ObjectInputStream clientStream = new ObjectInputStream(client.getInputStream());
				ClientAPIMsg msg = (ClientAPIMsg) clientStream.readObject();
				clientStream.close();
				client.close();
				Thread serviceThread = new Thread(new JTProcessRequest(msg, mapredJobs, lastJobId));
				serviceThread.start();
			} catch (IOException e) {
				System.out.println("JobTracker can't connect to client");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("JobTracker can't determine class of message received from client.");
				e.printStackTrace();
			}
		}
	}
	
	// Pretty printing of the state of cluster. for DEBUG
	/*
	private static void printState(){
		// print the jobs table
		System.out.println("");
		System.out.println("------------STATE OF CLUSTER------------------");
		for(JobTableEntry job : mapredJobs.values()){
			System.out.println("Job Id: " + job.getJob().getJobId() + " | Job Name: "  + job.getJob().getJobName() +
								" | Status: " + job.getStatus() +
								" | IP File Name: " + job.getJob().getIpFileName());
			System.out.println("\t\t---------Map Tasks--------------");
				for(TaskTableEntry mapTask : job.getMapTasks().values()){
					System.out.println("\t\t\tTask Id: " + mapTask.getTaskId() + " | Status: " + 
							mapTask.getStatus() + " | Node: " + mapTask.getCurrNodeId() +
							" | Start record: " + mapTask.getRecordRange().getFirst() + 
							" | End record: " + mapTask.getRecordRange().getSecond());
				}
				
		}
		System.out.println("------------END OF STATE------------------");
		System.out.println("");

	}
	*/
}
