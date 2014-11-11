package mapred;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Entry type for the jobs table
 */

public class JobTableEntry {

	// the mapreduce job object
	private MapReduceJob job;
	// the status of the job
	private String status;
	// the task list of mappers
	private ConcurrentHashMap<Integer, TaskTableEntry> mapTasks;
	// the task list of reducers
	private ConcurrentHashMap<Integer, TaskTableEntry> reduceTasks;
	
	public JobTableEntry(MapReduceJob job, String status){
		
		
		this.job = job;
		this.status = status;
		this.mapTasks = new ConcurrentHashMap<Integer, TaskTableEntry>();
		// this is assuming our system decides the number of mappers
		int numMappers = (job.getIpFileSize()/job.getSplitSize()) + 1;
		String initTaskStatus = "waiting";
		for(int i=0; i<numMappers; i++)		this.mapTasks.put(i, new TaskTableEntry(i, initTaskStatus));
	}
	
	public MapReduceJob getJob(){
		return this.job;
	}

	public String getStatus(){
		return this.status;
	}
	
	public ConcurrentHashMap<Integer, TaskTableEntry> getMapTasks(){
		return this.mapTasks;
	}
	
	public ConcurrentHashMap<Integer, TaskTableEntry> getReduceTasks(){
		return this.reduceTasks;
	}
	
	public void setJob(MapReduceJob job){
		this.job = job;
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public void setMapTasks(ConcurrentHashMap<Integer, TaskTableEntry> mapTasks){
		this.mapTasks = mapTasks;
	}
	
	public void setReduceTasks(ConcurrentHashMap<Integer, TaskTableEntry> mapTasks){
		this.reduceTasks = reduceTasks;
	}
	
}
