package mapred;

import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.types.JobTableEntry;
import mapred.types.TaskTableEntry;

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
	public void schedule(JobTableEntry nextJob, TaskTableEntry nextTask) {
		
		// choose next job
		int numJobs = this.mapredJobs.size();
		int nextJobId = (this.lastScheduledJob)%numJobs;
		int candidateJob = (this.lastScheduledJob+1)%numJobs;
		
		for(int i=0; i<numJobs; i++){
			if(this.mapredJobs.get(candidateJob).getStatus().equals("done"))
				candidateJob = (candidateJob++)%numJobs;
			else{
				// check if undispatched task exists
				String jobStatus = this.mapredJobs.get(nextJobId).getStatus();
				int candidateTask = 0;					// not valid task
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = null;
				
				if(!jobStatus.equals("reduce"))
					tasks = this.mapredJobs.get(nextJobId).getMapTasks();
				else
					tasks = this.mapredJobs.get(nextJobId).getReduceTasks();
					
				int numTasks = tasks.size();
				for(int j=1; j<=numTasks; j++){
					if(tasks.get(j).getStatus().equals("waiting")){
						candidateTask = j;
						break;
					}
				}
				
				if(!Integer.valueOf(candidateTask).equals(0)){
					nextJobId = candidateJob;
					break;
				}
			}
		}
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
