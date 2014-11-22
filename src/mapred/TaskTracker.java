package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;


import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

/*
 * Objects of this class run on the slave nodes in the cluster (Datanode), they communicate with the
 * JobTracker on the master node
 * It receives task jobs from the JobTracker and run them in different threads
 * It maintains a thread pool
 *  - If threads are available in the pool, it accepts the task and sends an ACK
 *  - If no threads are available, it rejects the task and sends back a NACK
 *  
 *  The Task objects are run in threads
 *  
 *  Once the allotted tasks are finished, it has to partition and then sort the 
 *  results by key 
 *  
 *  We should try to execute the tasks in separate JVMs ?
 */

public class TaskTracker {
	
	// handles to currently executing tasks (the runnables)
	// indexed by an id which is a string "jobId-taskId"
	private static ConcurrentHashMap<String, Task> runningTasks;
	// Maximum number of tasks allowed
	private static int maxRunningTasks;
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from JobTracker
	private static ServerSocket requestSocket;
	// server socket for responding to health report request
	private static ServerSocket pollingSocket;
	// port on TTMonitor where task sends finish message
	private static int monitorPort;
	// The IP Addr of the namenode
	private static String nameNode;					
	// The port of the namenode
	private static int nameNodePort;			
	// the ip addr of the JobTracker
	private static String jobtrackerIpAddr;
	// the port of the JobTracker
	private static int jobtrackerPort;
	// the port for datanode
	private static int dataNodePort;	
	// local base directory
	private static String localBaseDir;
	// block size of file chunks
	private static int blockSize;	
	// record size of files
	private static int recordSize;
	// split size of for mappers
	private static int splitSize;
	
	public static void main(String[] args){
		
		/* Do various init routines */
		runningTasks = new ConcurrentHashMap<String, Task>();
		mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
		initialize();
		
		// start the tasktracker monitoring thread
		Thread monitorThread = new Thread(new TTMonitor(mapredJobs, runningTasks, jobtrackerIpAddr, monitorPort));
		monitorThread.start();
		
		// start the tasktracker polling thread
		
		Thread pollingThread = new Thread(new TTPolling(pollingSocket));
		pollingThread.setDaemon(true);
		pollingThread.start();
		
		/* Start listening for commands and process them sequentially */
		while(true){
			try {
			// Listen for incoming commands
				Socket masterSocket = requestSocket.accept();
				ObjectInputStream masterStream = new ObjectInputStream(masterSocket.getInputStream());
				MasterToSlaveMsg command = (MasterToSlaveMsg) masterStream.readObject();
				masterStream.close();
				masterSocket.close();
				String commandType = command.getMsgType();
			
				// If launch job command
				if(commandType.equals("start")){
					// Decide whether to accept
					boolean accept = runningTasks.size() < maxRunningTasks;
					SlaveToMasterMsg replyMsg = new SlaveToMasterMsg();
					// If yes
					if(accept){
						// Launch execution thread
						String taskType = command.getTaskType();
						Task newTask;
						if(taskType.equals("map"))
							newTask = new Task(command.getIpFiles(), command.getJob(), 
												command.getTaskId(),
												command.getReadRecordStart(), command.getReadRecordEnd(),
												InetAddress.getLocalHost().getHostAddress(), monitorPort,
												recordSize, localBaseDir);
						else
							newTask = new Task(command.getIpFiles(), command.getJob(), 
												command.getTaskId(),
												InetAddress.getLocalHost().getHostAddress(), monitorPort, recordSize,
												nameNode, nameNodePort, dataNodePort, localBaseDir);
						
						Thread newExecutionThread = new Thread(newTask);
						newExecutionThread.start();
			
						// Modify mapredJobs
						JobTableEntry jobEntry;
						// check if entry for this job already exists
						jobEntry = mapredJobs.get(command.getJob().getJobId());
						if(jobEntry == null)	jobEntry = new JobTableEntry(command.getJob(), taskType,
																	blockSize, recordSize, splitSize);
						
						// add the appropriate task entry
						// Assume that the following taskEntry doesn't exist in the job table
						// so it will always be new
						TaskTableEntry taskEntry;
						if(taskType.equals("map")){
							taskEntry = new TaskTableEntry(command.getTaskId(), "running", "map");
							taskEntry.setCurrNodeId(InetAddress.getLocalHost().getHostAddress());
							Pair<Integer, Integer> recordRange = new Pair<Integer, Integer>();
							recordRange.setFirst(command.getReadRecordStart());
							recordRange.setSecond(command.getReadRecordEnd());
							taskEntry.setRecordRange(recordRange);
							jobEntry.getMapTasks().put(command.getTaskId(), taskEntry);
						}
						else{
							taskEntry = new TaskTableEntry(command.getTaskId(), "running", "reduce");
							taskEntry.setCurrNodeId(InetAddress.getLocalHost().getHostAddress());
							jobEntry.getReduceTasks().put(command.getTaskId(), taskEntry);
						}
						mapredJobs.put(command.getJob().getJobId(), jobEntry);
						
						// modify runningTasks
						String id = Integer.toString(command.getJob().getJobId()) + "-" + 
												Integer.toString(command.getTaskId());
						runningTasks.put(id, newTask);
						
						// Set reply to "accept"
						replyMsg.setMsgType("accept");
						
					}
					// If no
					else{
						// Set reply to "reject"
						replyMsg.setMsgType("reject");
					}
					
					Socket responseSocket = new Socket(jobtrackerIpAddr, jobtrackerPort);
					ObjectOutputStream responseStream = new ObjectOutputStream(responseSocket.getOutputStream());
					responseStream.writeObject(replyMsg);
					responseStream.close();
					responseSocket.close();
				}
				// If stop job command
				else{
				// Find all execution tasks for this job and kill them
				JobTableEntry removeEntry = mapredJobs.get(command.getJobStopId());
				ConcurrentHashMap<Integer, TaskTableEntry> removeTasks = null;
					if(removeEntry != null)	{
						 removeTasks = removeEntry.getMapTasks();
					}
					for(Integer task : removeTasks.keySet()){
						String removeId = Integer.toString(command.getJobStopId()) 
												+ "-" + task.toString();
						if(runningTasks.containsKey(removeId)){
							Task killTask = runningTasks.get(removeId);
							killTask.killThread();
							runningTasks.remove(removeId);
						}
					}
				mapredJobs.remove(removeEntry);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.out.println("Class for MasterToSlaveMsg not found.");
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
				else if(key.equals("JobTrackerHost")){
					//jobtrackerIpAddr = value;
					jobtrackerIpAddr = InetAddress.getLocalHost().getHostAddress(); // for testing
				}
				else if(key.equals("SlaveToDispatcherSocket")){
					jobtrackerPort = Integer.parseInt(value);
				}
				else if(key.equals("LocalBaseDir")){
					localBaseDir = value;
				}
				else if(key.equals("DN-RegistryPort")){
					dataNodePort = Integer.parseInt(value);
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
				else if(key.equals("DispatcherToSlaveSocket")){
					requestSocket = new ServerSocket(Integer.parseInt(value));
				}
				else if(key.equals("PollingSocket")){
					pollingSocket = new ServerSocket(Integer.parseInt(value));
				}
				else if(key.equals("TaskToTTMonitorSocket")){
					monitorPort = Integer.parseInt(value);
				}
				else if(key.equals("MaxTasksPerNode")){
					maxRunningTasks = Integer.parseInt(value);
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

}
