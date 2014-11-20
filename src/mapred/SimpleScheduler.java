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
	private ConcurrentHashMap<Integer,JobTableEntry> mapredJobs;
	// Last scheduled job, for Round Robin Scheduler
	private int lastScheduledJob;
	// Last scheduled task, for above job
	private int lastScheduledTask;
	
	// constructor with handle to mapredJobs
	public SimpleScheduler(ConcurrentHashMap<Integer,JobTableEntry> mapredJobs){
		this.mapredJobs = mapredJobs;
	}
	

	@Override
	public void schedule(JobTableEntry nextJob, TaskTableEntry nextTask) {
		
		// choose next job
		int numJobs = this.mapredJobs.size();
		//System.out.println("scheduler numJobs: " + numJobs);
		int nextJobId = 0;
		// check if no jobs are in mapredJobs
		if(numJobs == 0)	return;
		else {
			//System.out.println("Scheduler: Jobs available");
			//nextJobId = ((this.lastScheduledJob)%numJobs) + 1;
		}
		
		int candidateJob = ((this.lastScheduledJob+1)%numJobs)+1;
		//System.out.println("last: " + this.lastScheduledJob);
		//System.out.println("cand: " + candidateJob);
		//System.out.println("num: " + numJobs);
		for(int i=0; i<numJobs; i++){
			System.out.println(this.mapredJobs.get(candidateJob));
			if(this.mapredJobs.get(candidateJob).getStatus().equals("done"))
				candidateJob = ((candidateJob++)%numJobs)+1;
			else{
				nextJobId = candidateJob;
				// check if undispatched task exists
				String jobStatus = this.mapredJobs.get(nextJobId).getStatus();
				int candidateTask = 0;					// not valid task
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = null;
				
				if(!jobStatus.equals("reduce"))
					tasks = this.mapredJobs.get(nextJobId).getMapTasks();
				else
					tasks = this.mapredJobs.get(nextJobId).getReduceTasks();
					
				int numTasks = tasks.size();
				System.out.println(numTasks);
				for(int j=0; j<numTasks; j++){
					System.out.println("Task status for " + j + " is " + tasks.get(j).getStatus());
					if(tasks.get(j).getStatus().equals("waiting")){
						candidateTask = j;
						break;
					}
				}
				
				if(!Integer.valueOf(candidateTask).equals(0)){
					// schedule this job
					nextJobId = candidateJob;
					break;
				}
			}
		}
	}
	
	public void setLastScheduledJob(int lastScheduledJob){
		//System.out.println("set last to : " + lastScheduledJob);
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
