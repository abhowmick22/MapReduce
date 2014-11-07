package mapred;

/*
 * Object of this class on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker implements Runnable{

	// Scheduler for allotting jobs on the slave nodes
	private Scheduler scheduler;
	// The runnable object which monitors 
	private JMonitor monitor;
	
	
	/* Configurations for the cluster */
	

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		Thread monitorThread = new Thread(monitor);
		
	}
	
	public void init(){
		
	}
	
}
