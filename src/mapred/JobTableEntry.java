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
	private ConcurrentHashMap<String, TaskTableEntry> mapTasks;
	// name of ip file for the job
	private String ipFileName;
	
	public MapReduceJob getJob(){
		return this.job;
	}

	public String getStatus(){
		return this.status;
	}
	
	public ConcurrentHashMap<String, TaskTableEntry> getMapTasks(){
		return this.mapTasks;
	}
	
	public String getIpFilename(){
		return this.ipFileName;
	}
	
	public void setJob(MapReduceJob job){
		this.job = job;
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public void setMapTasks(ConcurrentHashMap<String, TaskTableEntry> mapTasks){
		this.mapTasks = mapTasks;
	}
	
	public void setIpFilename(String ipFileName){
		this.ipFileName = ipFileName;
	}
	
}
