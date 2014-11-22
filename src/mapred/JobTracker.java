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
	// list of active cluster nodes, basically a map from nodeid to list of TaskTableEntry objects
	private static ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>> activeNodes;
	// The IP Addr of the namenode
	private static String nameNode;
	// the port of the namenode
	private static int nameNodePort;
	// port to which send data to client to
	private static int respondClientPort;
	// port to dispatch tasks
	private static int dispatchPort;
	// port where JTPolling should poll for health reports
	private static int pollingPort;
	// block size of file chunks
	private static int blockSize;
	// record size of files
	private static int recordSize;
	// split size of for mappers
	private static int splitSize;

	public static void main(String[] args) {

		// Do various init routines
		// initialise empty jobs list
		mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
		clusterNodes = new ConcurrentHashMap<String, Pair<String, Integer>>();
		activeNodes = new ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>>();
		lastJobId = 0;
				
		initialize();
		
		// start the jobtracker monitoring thread
		Thread monitorThread = new Thread(new JTMonitor(mapredJobs, clusterNodes, monitorSocket));
		monitorThread.start();
		
		// start the jobtracker dispatcher thread
		Thread dispatcherThread = new Thread(new JTDispatcher(mapredJobs, activeNodes, clusterNodes, nameNode, 
												nameNodePort, dispatcherAckSocket, dispatchPort));
		dispatcherThread.start();
		
		// start the polling thread
		
		Thread pollingThread = new Thread(new JTPolling(mapredJobs, activeNodes, clusterNodes, 
												pollingPort, nameNode, nameNodePort));
		pollingThread.start();
		
		// Start listening for mapreduce jobs from clientAPI
		while(true){
			try {
				Socket client = clientAPISocket.accept();
				ObjectInputStream clientStream = new ObjectInputStream(client.getInputStream());
				ClientAPIMsg msg = (ClientAPIMsg) clientStream.readObject();
				clientStream.close();
				client.close();
				Thread serviceThread = new Thread(new JTProcessRequest(msg, mapredJobs, lastJobId, blockSize,
													recordSize, splitSize, respondClientPort));
				serviceThread.start();
			} catch (IOException e) {
				System.out.println("JobTracker can't connect to client");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.out.println("JobTracker can't determine class of message received from client.");
			}
		}
	}
	
	public static void initialize(){

		try {
			
			// Filepath of config file
			String filePath = System.getProperty("user.dir") + System.getProperties().get("file.separator").toString()
								+ "tempDfsConfigFileCopy";
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
					for(int i=0; i<nodes.length; i++){
						p = new Pair<String, Integer>();
						p.setFirst("up");
						p.setSecond(0);
						//TODO: proper logic for initializing clusterNodes and activeNodes
						/*
						clusterNodes.put(nodes[i], p);
						activeNodes.put(nodes[i], new ArrayList<Pair<JobTableEntry, TaskTableEntry>>());
						*/
						// for testing
						clusterNodes.put(InetAddress.getLocalHost().getHostAddress(), p);
						activeNodes.put(InetAddress.getLocalHost().getHostAddress(), 
														new ArrayList<Pair<JobTableEntry, TaskTableEntry>>());
					}
				}
				else if(key.equals("RecordSize")){
					recordSize = Integer.parseInt(value);
				}
				else if(key.equals("BlockSize")){
					blockSize = Integer.parseInt(value);
				}
				else if(key.equals("SplitSize")){
					splitSize = Integer.parseInt(value);
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
				else if(key.equals("JobTrackerToClientSocket")){
					respondClientPort = Integer.parseInt(value);
				}
				else if(key.equals("DispatcherToSlaveSocket")){
					dispatchPort = Integer.parseInt(value);
				}
				else if(key.equals("PollingSocket")){
					pollingPort = Integer.parseInt(value);
				}
				else if(key.charAt(0) == '#'){
					continue;				// this is a comment
				}
				else{
					;
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("JobTracker: Could not find the config file.");
		} catch (IOException e) {
			System.out.println("JobTracker: Could not read the config file.");
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
