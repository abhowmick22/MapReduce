package mapred;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Entry type for the tasks tables
 */

public class TaskTableEntry {
	// the task id
	private int taskId;
	// the status of the task
	private String status;
	// the id of the node on which this task is running/finished
	private String currNodeId;
	// the list of op file paths according to the partition
	// (each partition has one file path)
	private ConcurrentHashMap<Integer, String> opFileNames;
	
	public TaskTableEntry(int taskId, String status){
		this.taskId = taskId;
		this.status = status;
		this.currNodeId = null;
		this.opFileNames = new ConcurrentHashMap<Integer, String>();
	}
	
	public int getTaskId(){
		return this.taskId;
	}
	
	public String getStatus(){
		return this.status;
	}
	
	public String getCurrNodeId(){
		return this.currNodeId;
	}
	
	public ConcurrentHashMap<Integer, String> getOpFileNames(){
		return this.opFileNames;
	}
	
	public void setTaskId(int taskId){
		this.taskId = taskId;
	}
	
	public void setStatus(String status){
		this.status = status;
	}
	
	public void setCurrNodeId(String currNodeId){
		this.currNodeId = currNodeId;
	}
	
	public void setOpFileNames(ConcurrentHashMap<Integer, String> opFileNames){
		this.opFileNames = opFileNames;
	}

}
