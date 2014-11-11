package mapred;

import java.util.List;

/*
 * Objects of this instance are units of computation
 * this is run in a thread by the TaskTracker
 * 
 * Right now, we assume that a single task works on an entire file
 * chunk, there is no further splitting among partitions for tasks
 */

public class Task implements Runnable{
	
	// flag indicating whether this task should continue executing
	private volatile boolean alive;
	// The file block which this task takes as input
	private List<String> ipFileNames;
	// The job of which this task is part
	private MapReduceJob parentJob;
	// Type of task
	private String taskType;
	// Task Id
	private int taskId;
	// record numbers to read in case of map task
	private int readRecordStart;
	private int readRecordEnd;
	
	// Special constructor to create a map Task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId,
						int readRecordStart, int readRecordEnd){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "map"
		this.taskId = taskId;
		this.readRecordStart = readRecordStart;
		this.readRecordEnd = readRecordEnd;
	}
	
	// Special constructor to create a reduce task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "reduce"
		this.taskId = taskId;
	}
	
	
	@Override
	public void run() {
		
		this.alive = true;
		
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void partition(){
		
	}
	
	// sort the keys within each partition before feeding into reducer
	public void sort(){
		
	}
	
	// method to kill the thread running this Runnable
	// this will be called by tasktracker, which resets the alive flag
	// The flag is conitnually checked by the run method, which exits if it is set to false
	// ?? Returns true if the thread was successfully killed
	public void killThread(){
		this.alive = false;
		//while(!this.alive) ;	// cause the taskTracker to be spinning till the thread dies
		//return true;
	}

}
