package mapred;

import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.types.JobTableEntry;
import mapred.types.Pair;
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
	public Pair<JobTableEntry, TaskTableEntry> schedule() {
		
		// choose next job
		int numJobs = this.mapredJobs.size();
		//System.out.println("scheduler numJobs: " + numJobs);
		int nextJobId;
		// check if no jobs are in mapredJobs
		// to be returned
		Pair<JobTableEntry, TaskTableEntry> next = null;
		
		if(numJobs == 0)	return next;
		else {
			//System.out.println("Scheduler: Jobs available");
			//nextJobId = ((this.lastScheduledJob)%numJobs) + 1;
		}
		
		//int candidateJob = ((this.lastScheduledJob+1)%numJobs)+1;
		//int candidateJob = 0;
		//System.out.println("last: " + this.lastScheduledJob);
		//System.out.println("cand: " + candidateJob);
		//System.out.println("num: " + numJobs);
		for(int i=0; i<numJobs; i++){
			//System.out.println("scheduler: " + this.mapredJobs.size() + "-" + this.mapredJobs.get(i));
			String status = this.mapredJobs.get(i).getStatus();
			if(status.equals("done")){
				//candidateJob = ((candidateJob++)%numJobs)+1;
				
				continue;
			}
			else{			// "waiting" or "map" or "reduce"
				//System.out.println(this.mapredJobs.get(i).getStatus());
				nextJobId = this.mapredJobs.get(i).getJob().getJobId();
				// check if undispatched task exists
				String jobStatus = this.mapredJobs.get(i).getStatus();
				int nextTaskId = -1;				// not valid task
				String nextTaskType;
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = null;
				
				if(!jobStatus.equals("reduce")){
					tasks = this.mapredJobs.get(i).getMapTasks();
					nextTaskType = "map";
				}
				else{
					//System.out.println("scheduler: job in reduce phase");
					tasks = this.mapredJobs.get(i).getReduceTasks();
					nextTaskType = "reduce";
				}
					
				int numTasks = tasks.size();
				boolean taskAvl = false;
				//System.out.println(numTasks);
				for(int j=0; j<numTasks; j++){
					//System.out.println("Task status for " + j + " is " + tasks.get(j).getStatus());
					if(tasks.get(j).getStatus().equals("waiting")){
						nextTaskId = j;
						//System.out.println("pos of scheduled task in table is " + j);
						//System.out.println("its id is " + tasks.get(j).getTaskId());
						taskAvl = true;
					}
				}
				
				
				if(!Integer.valueOf(nextTaskId).equals(-1) && taskAvl){
					//System.out.println("candidate task is " + nextTaskId);
					// schedule this job
					//nextJobId = candidateJob;
					this.mapredJobs.get(i).setStatus(nextTaskType);
					tasks.get(nextTaskId).setStatus("running");
					next = new Pair<JobTableEntry, TaskTableEntry>();
					next.setFirst(this.mapredJobs.get(i));
					next.setSecond(tasks.get(nextTaskId));
					//System.out.println();
					//nextJob.setStatus(nextTaskType);
					break;
				}
				
			}
		}
		return next;
	
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
