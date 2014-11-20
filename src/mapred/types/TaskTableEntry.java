package mapred.types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Entry type for the tasks tables
 */

public class TaskTableEntry {
	// the task id
	private int taskId;
	// type of the task
	private String taskType;
	// the status of the task
	private String status;
	// the id of the node on which this task is running/finished
	private String currNodeId;
	// the list of op file paths according to the partition
	// (each partition has one file path)
	private ConcurrentHashMap<Integer, String> opFileNames;
	// record range for each task, [start, stop]
	private Pair<Integer, Integer> recordRange;


	
	public TaskTableEntry(int taskId, String status, String taskType){
		this.taskId = taskId;
		this.status = status;
		this.taskType = taskType;
		this.currNodeId = null;
		this.opFileNames = new ConcurrentHashMap<Integer, String>();
		this.recordRange = new Pair<Integer, Integer>();
		this.recordRange.setFirst(0);
		this.recordRange.setSecond(0);
	}
	
	public int getTaskId(){
		return this.taskId;
	}
	
	public String getTaskType(){
		return this.taskType;
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
	
	public Pair<Integer, Integer> getRecordRange(){
		return this.recordRange;
	}
	
	public void setTaskId(int taskId){
		this.taskId = taskId;
	}
	
	public void setTaskType(String taskType){
		this.taskType = taskType;
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
	
	public void setRecordRange(Pair<Integer, Integer> recordRange){
		this.recordRange = recordRange;
	}

}
