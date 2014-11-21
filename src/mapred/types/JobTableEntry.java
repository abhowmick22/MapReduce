package mapred.types;

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
	// the number of pending map tasks
	private int pendingMaps;
	// the number of pending reduce tasks
	private int pendingReduces;
	
	public JobTableEntry(MapReduceJob job, String status){		
		this.job = job;
		this.status = status;
		this.mapTasks = new ConcurrentHashMap<Integer, TaskTableEntry>();
		this.reduceTasks = new ConcurrentHashMap<Integer, TaskTableEntry>();
		
		// this assumes that our system decides the number of mappers
		int numMappers = job.getIpFileSize()/job.getSplitSize();
		if(job.getIpFileSize()%job.getSplitSize() != 0)
			numMappers++;
		String initTaskStatus = "waiting";
		// populate map tasks table
		int numRecordsInFile = job.getIpFileSize()/job.getRecordSize();
		int numRecordsPerSplit = job.getSplitSize()/job.getRecordSize();
		for(int i=0; i<numMappers; i++){
			this.mapTasks.put(i, new TaskTableEntry(i, initTaskStatus, "map"));
			this.mapTasks.get(i).getRecordRange().setFirst(numRecordsPerSplit*i);
			this.mapTasks.get(i).getRecordRange().setSecond(Math.min(numRecordsPerSplit*i + 
										numRecordsPerSplit - 1, numRecordsInFile - 1));
		}
		// populate reduce tasks table
		int numReducers = job.getNumReducers();
		for(int i=0; i<numReducers; i++){
			this.reduceTasks.put(i, new TaskTableEntry(i, initTaskStatus, "reduce"));
		}
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
	
	public int getPendingMaps(){
		return this.pendingMaps;
	}
	
	public int getPendingReduces(){
		return this.pendingReduces;
	}
	
	public void decPendingMaps(){
		this.pendingMaps--;
	}
	
	public void decPendingReduces(){
		this.pendingReduces--;
	}
	
	public void incPendingMaps(){
		this.pendingMaps++;
	}
	
	public void incPendingReduces(){
		this.pendingReduces++;
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
	
	public void setReduceTasks(ConcurrentHashMap<Integer, TaskTableEntry> reduceTasks){
		this.reduceTasks = reduceTasks;
	}
	
	public void setPendingMaps(int pendingMaps){
		this.pendingMaps = pendingMaps;
	}
	
	public void setPendingReduces(int pendingReduces){
		this.pendingReduces = pendingReduces;
	}
	
}
