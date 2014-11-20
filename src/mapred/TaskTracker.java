package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
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
	// the ip addr of the JobTracker
	// this should again be read in from a config file
	private static String jobtrackerIpAddr;
	
	public static void main(String[] args){
		
		try {
			/* Do various init routines */
			runningTasks = new ConcurrentHashMap<String, Task>();
			// TODO: Read in this parameter from a config file instead of hardcoding 
			maxRunningTasks = 10; 
			// TODO: Init to proper jobtracker
			jobtrackerIpAddr = InetAddress.getLocalHost().getHostAddress();
			
			// initialize empty jobs list
			mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
			// initialize master socket
			requestSocket = new ServerSocket(10001);
			// start the tasktracker monitoring thread
			Thread monitorThread = new Thread(new TTMonitor(mapredJobs, runningTasks, jobtrackerIpAddr));
			monitorThread.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Start listening for commands and process them sequentially */
		while(true){
			try {
			// Listen for incoming commands
				System.out.println("TaskTracker at " + InetAddress.getLocalHost().getHostAddress() + " : Listening...");
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
												command.getTaskType(), command.getTaskId(),
												command.getReadRecordStart(), command.getReadRecordEnd(),
												InetAddress.getLocalHost().getHostAddress());
						else
							newTask = new Task(command.getIpFiles(), command.getJob(), 
												command.getTaskType(), command.getTaskId(),
												InetAddress.getLocalHost().getHostAddress());
						
						Thread newExecutionThread = new Thread(newTask);
						newExecutionThread.start();
			
						// Modify mapredJobs
						JobTableEntry jobEntry;
						// check if entry for this job already exists
						jobEntry = mapredJobs.get(command.getJob().getJobId());
						if(jobEntry == null)	jobEntry = new JobTableEntry(command.getJob(), taskType);
						
						// add the appropriate task entry
						TaskTableEntry taskEntry;
						// Assume that the following taskEntry doesn't exist in the job table
						// so it will always be new
						if(taskType.equals("map")){
							taskEntry = new TaskTableEntry(command.getTaskId(), "running", "map");
							taskEntry.setCurrNodeId(InetAddress.getLocalHost().getHostAddress());
							List<Integer> recordRange = new ArrayList<Integer>();
							recordRange.add(0, command.getReadRecordStart());
							recordRange.add(1, command.getReadRecordEnd());
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
					
					Socket responseSocket = new Socket(jobtrackerIpAddr, 10000);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

}
