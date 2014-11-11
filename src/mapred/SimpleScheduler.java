package mapred;

import java.util.concurrent.ConcurrentHashMap;

import mapred.interfaces.Scheduler;

/*
 * This object makes scheduling decisions - i.e it decides which jobs to be scheduled next 
 * It has very simple logic
 */

public class SimpleScheduler implements Scheduler{
	
	// list of mapredJobs to be scheduled
	private ConcurrentHashMap<String, JobTableEntry> mapredJobs;
	// Last scheduled job, for Round Robin Scheduler
	private int lastScheduledJob;
	// Last scheduled task, for above job
	private int lastScheduledTask;
	

	@Override
	public void schedule(int jobId, int taskId, String nodeId, String nextTaskType) {
		
		
	}
	
	public void setLastScheduledJob(int lastScheduledJob){
		this.lastScheduledJob = lastScheduledJob;
	}
	
	public void setLastScheduledTask(int lastScheduledTask){
		this.lastScheduledTask = lastScheduledTask;
	}

	public int getLastScheduledJob(){
		return this.lastScheduledJob;
	}
	
	public int getLastScheduledTask(){
		return this.lastScheduledTask;
	}
}
