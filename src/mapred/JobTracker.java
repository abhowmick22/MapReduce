package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
	// dispatcher ack socket
	private static ServerSocket dispatcherAckSocket;
	// monitor server socket
	private static ServerSocket monitorSocket;
	// id of last launched job
	private static int lastJobId;
	// map of cluster nodes read from config file, each has a pair as value
	// first element of pair is status (up/down), second element is load (Integer)
	// assume default status is up
	private static ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	// The IP Addr of the namenode, read from config file
	private static String nameNode;
	// the port of the namenode, read from config file
	private static int nameNodePort;
	// local base directory
	private static String localBaseDir;
	// block size of file chunks
	private static int ipBlockSize;
	// record size of files
	private static int ipRecordSize;

	
	public static void main(String[] args) {
		
		initialize();
		
		// TODO : Initialize list of clusters from config file
		//clusterNodes = new ConcurrentHashMap<String, Pair<String, Integer>>();
		// for testing, just add this node to the list
		//Pair<String, Integer> temp = new Pair<String, Integer>();
		//temp.setFirst("up");
		//temp.setSecond(0);
		//clusterNodes.put(InetAddress.getLocalHost().getHostAddress(), temp);
		// read namenode from config file
		// for testing, just let this node be namenode
		//nameNode = InetAddress.getLocalHost().getHostAddress();
		
		// Do various init routines
		// initialise empty jobs list
		mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
		lastJobId = 0;
		// initialise clientAPI socket
		//clientAPISocket = new ServerSocket(20000);
		
		// start the jobtracker monitoring thread
		JTMonitor jtm = new JTMonitor(mapredJobs, clusterNodes, monitorSocket);
		Thread monitorThread = new Thread(jtm);
		monitorThread.start();
		
		// start the jobtracker dispatcher thread
		Thread dispatcherThread = new Thread(new JTDispatcher(mapredJobs, clusterNodes, nameNode, 
												dispatcherAckSocket));
		dispatcherThread.start();
		
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
	
	public static void initialize(){

		try {
			
			// Filepath of config file
			String filePath = System.getProperty("user.dir") + System.getProperties().get("file.separator").toString()
								+ "tempDfsConfigFile";
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String config, key, value;
			while((config = reader.readLine()) != null){
				String[] tokens = config.split("\\s*=\\s*");
				if(tokens.length < 2)	continue;
				key = tokens[0];
				value = tokens[1];
				// Initialize configs accordingly
				if(key.equals("DFS-RegistryPort")){
					nameNodePort = Integer.parseInt(value);
				}
				else if(key.equals("DFS-RegistryHost")){
					nameNode = value;
				}
				else if(key.equals("DataNodeNames")){
					String[] nodes = value.split("\\s*,\\s*");
					if(nodes.length < 1)	System.out.println("No data nodes are active.");
					Pair<String, Integer> p = null;
					clusterNodes = new ConcurrentHashMap<String, Pair<String, Integer>>();
					for(int i=0; i<nodes.length; i++){
						p = new Pair<String, Integer>();
						p.setFirst("up");
						p.setSecond(0);
						//TODO: clusterNodes.put(nodes[i], p);
						// for testing
						clusterNodes.put(InetAddress.getLocalHost().getHostAddress(), p);
					}
				}
				else if(key.equals("LocalBaseDir")){
					localBaseDir = value;
				}
				else if(key.equals("RecordSize")){
					ipRecordSize = Integer.parseInt(value);
				}
				else if(key.equals("BlockSize")){
					ipBlockSize = Integer.parseInt(value);
				}
				else if(key.equals("ClientToJobTrackerSocket")){
					clientAPISocket = new ServerSocket(Integer.parseInt(value));
				}
				else if(key.equals("SlaveToDispatcherSocket")){
					dispatcherAckSocket = new ServerSocket(Integer.parseInt(value));
				}
				else if(key.equals("JobTrackerMonitorSocket")){
					monitorSocket = new ServerSocket(Integer.parseInt(value));
				}
				else if(key.charAt(0) == '#'){
					continue;				// this is a comment
				}
				else{
					;
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
