package mapred;

import java.util.concurrent.ConcurrentHashMap;

import mapred.interfaces.Scheduler;

/*
 * This object is responsible for continuously seeking out new tasks
 * to schedule and dispatching them on the cluster
 */

public class JTDispatcher implements Runnable {
	
	// Scheduler for allotting jobs on the slave nodes
	private Scheduler scheduler;
	// handle to the jobtracker's mapredJobs
	private ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	
	public JTDispatcher(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs){
		this.mapredJobs = mapredJobs;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
