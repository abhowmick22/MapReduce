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
	
	public JobTableEntry(MapReduceJob job, String status, 
				ConcurrentHashMap<Integer, TaskTableEntry> mapTasks){
		this.job = job;
		this.status = status;
		this.mapTasks = mapTasks;
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
	
	public void setJob(MapReduceJob job){
		this.job = job;
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public void setMapTasks(ConcurrentHashMap<Integer, TaskTableEntry> mapTasks){
		this.mapTasks = mapTasks;
	}
	
}
