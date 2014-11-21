package mapred;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

/*
 * This object makes scheduling decisions - i.e it decides which jobs to be scheduled next 
 * It has very simple logic
 * 
 * TODO: Logic to return locations for map and reduce tasks
 */

public class SimpleScheduler implements Scheduler{
	
	// list of mapredJobs to be scheduled
	private ConcurrentHashMap<Integer,JobTableEntry> mapredJobs;
	// Last scheduled job, for Round Robin Scheduler
	private int lastScheduledJob;
	// Last scheduled task, for above job
	private int lastScheduledTask;
	// IP Addr for namenode
	private String nameNode;
	// map of all nodes in the system with status and load
	private ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	
	// constructor with handle to mapredJobs
	public SimpleScheduler(ConcurrentHashMap<Integer,JobTableEntry> mapredJobs, 
								ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
								String nameNode){
		this.mapredJobs = mapredJobs;
		this.clusterNodes = clusterNodes;
		this.nameNode = nameNode;
	}
	

	@Override
	public Pair<JobTableEntry, TaskTableEntry> schedule() {
		
		// choose next job
		int numJobs = this.mapredJobs.size();

		Pair<JobTableEntry, TaskTableEntry> next = null;
		
		// check if no jobs are in mapredJobs
		// to be returned
		if(numJobs == 0)	return next;
		
		for(int i=0; i<numJobs; i++){
			String status = this.mapredJobs.get(i).getStatus();
			if(status.equals("done")){
				continue;
			}
			else{			// "waiting" or "map" or "reduce"
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
					tasks = this.mapredJobs.get(i).getReduceTasks();
					nextTaskType = "reduce";
				}
					
				int numTasks = tasks.size();
				boolean taskAvl = false;				// flag indicating if any tasks available
				for(int j=0; j<numTasks; j++){
					if(tasks.get(j).getStatus().equals("waiting")){
						nextTaskId = j;
						taskAvl = true;
					}
				}
				
				// If a dispatchable task is available
				if(taskAvl){
					// return this job
					//nextJobId = candidateJob;
					this.mapredJobs.get(i).setStatus(nextTaskType);
					tasks.get(nextTaskId).setStatus("running");
					next = new Pair<JobTableEntry, TaskTableEntry>();
					next.setFirst(this.mapredJobs.get(i));
					next.setSecond(tasks.get(nextTaskId));
					break;
				}
			}
		}
		return next;
	
	}
	
	// TODO: Implement this
	// return the IP Addr to which we need to send the mapper
	public String getBestMapLocation(){
		String result =  null;
		try {
			
			// Get the locality information 
			
			
			
			
			
			
			
			
			
			
			
			
			 result = InetAddress.getLocalHost().getHostAddress();		// placeholder for testing
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	// TODO: Implement this
	// return the IP Addr to which we need to send the reducer
	public String getBestReduceLocation(){
		String result =  null;
		try {
			 result = InetAddress.getLocalHost().getHostAddress();		// placeholder for testing
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
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
