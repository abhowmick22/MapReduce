package mapred;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

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
 *  Do we need to execute the tasks in separate jvms ?
 */

public class TaskTracker {
	
	// Number of current tasks
	private int numRunningTasks;
	// Maximum number of tasks allowed
	private int maxRunningTasks;
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from JobTracker
	private static ServerSocket masterSocket;
	
	public static void main(String[] args){
		
		try {
			/* Do various init routines */
			// initialize empty jobs list
			mapredJobs = new ConcurrentHashMap<Integer, JobTableEntry>();
			// initialize master socket
			masterSocket = new ServerSocket(10001);
			// start the tasktracker monitoring thread
			Thread monitorThread = new Thread(new TTMonitor());
			monitorThread.run();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Start listening for commands and process them sequentially */
		while(true){
			// Listen for incoming commands
			
			
			// If launch job command
				// Decide whether to accept
					// If yes
						// Launch execution thread
			
						// Modify mapredJobs and numRunningTasks
			
						// Send back "accept" response
			
					// If no
						// Send back "reject" response
			
			// If stop job command
				// Find all execution threads for this job
			
				// Kill corresponding threads
			
				// Modify mapredJobs and numRunningTasks
			
		}
		
	}

}
