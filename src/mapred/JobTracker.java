package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.messages.ClientAPIMsg;
import mapred.types.JobTableEntry;
import mapred.types.TaskTableEntry;

/*
 * Single object of this class runs on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker{
	
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from clientAPI
	private static ServerSocket clientAPISocket;
	private static ServerSocket monitorSocket;
	// id of last launched job
	private static int lastJobId;
	// Data Structure to store workload on each node
	private static ConcurrentHashMap<String, Integer> clusterLoad;
	// List of Clusters
	private static List<String> clusterNodes;
	
	/* TODO: Configurations for the cluster */

	public static void main(String[] args) {
		
		// TODO : Read list of clusters from config file
		clusterNodes = new ArrayList<String>();
		
		// Do various init routines
		try {
			// initialise empty jobs list
			mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
			// initialise clusterLoad info
			for(String node : clusterNodes)	clusterLoad.put(node, 0);
			
			lastJobId = 0;
			
			// initialise clientAPI socket
			clientAPISocket = new ServerSocket(20000);
			
			// start the jobtracker monitoring thread
			monitorSocket = new ServerSocket(10002);
			JTMonitor jtm = new JTMonitor(mapredJobs, clusterLoad, monitorSocket);
			Thread monitorThread = new Thread(jtm);
			monitorThread.start();
			
			// start the jobtracker dispatcher thread
			Thread dispatcherThread = new Thread(new JTDispatcher(mapredJobs, clusterLoad));
			dispatcherThread.start();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Start listening for mapreduce jobs from clientAPI
		while(true){
			try {
				System.out.println("Listening...");
				Socket client = clientAPISocket.accept();
				ObjectInputStream clientStream = new ObjectInputStream(client.getInputStream());
				ClientAPIMsg msg = (ClientAPIMsg) clientStream.readObject();
				clientStream.close();
				client.close();
				System.out.println("Read clientAPI message with request type " + msg.getCommand());
				Thread serviceThread = new Thread(new JTProcessRequest(msg, mapredJobs, lastJobId));
				serviceThread.run();
				printState();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	// Pretty printing of the state of cluster
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
							" | Start record: " + mapTask.getRecordRange().get(0) + 
							" | End record: " + mapTask.getRecordRange().get(1));
				}
				
		}
		System.out.println("------------END OF STATE------------------");
		System.out.println("");

	}
	
}
