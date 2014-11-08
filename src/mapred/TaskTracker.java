package mapred;

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
	
	public static void main(String[] args){
		
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void partition(){
		
	}
	
	// sort the keys within each partition before feeding into reducer
	public void sort(){
		
	}

}
