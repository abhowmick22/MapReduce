package mapred;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Object of this class runs on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker{

	// Scheduler for allotting jobs on the slave nodes
	private Scheduler scheduler;
	// The runnable object which monitors 
	private JMonitor monitor;
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private ConcurrentHashMap mapredJobs;
	
	/* Configurations for the cluster */

	public static void main(String[] args) {
		
		
		// server socket for listening from clientAPI
		
		
		// start the jobtracker monitoring thread
		monitor = new JMonitor();
		Thread monitorThread = new Thread(monitor);
		monitorThread.run();
		
		
		
	}
	
}
