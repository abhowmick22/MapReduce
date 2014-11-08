package mapred;

/*
 * Object of this class on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker{

	// Scheduler for allotting jobs on the slave nodes
	private Scheduler scheduler;
	// The runnable object which monitors 
	private static JMonitor monitor;
	
	/* Configurations for the cluster */

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Thread monitorThread = new Thread(monitor);
		
	}
	
	public void init(){
		
	}
	
}
