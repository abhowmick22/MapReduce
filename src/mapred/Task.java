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
		
		// If this is a map task
		
			// Get the mapper from the parent job
		
			// Initialize a RecordReader supplying (readRecordStart, readRecordEnd)
		
			// Initialize an OutputSet 
		
			// loop till RecordReader returns null
				// for every record returned
		
				// call the map method of mapper, supplying record and collecting output in OutputSet
		
			// Determine the number of reducers (R) from the parent mapreduce job
		
			// shuffle (partition) the KV pairs from OutputSet into R lists
		
			//  Flush them onto disk, each such file contains all KV pairs per partition (one per line)
		
			// Notify the namenode, and ask to add this
		
			// Indicate that it is finished to JobTracker (JTMonitor)
		
		
		// If this is a reduce task
		
			// get the reducer from the parent job
		
			// already have list of ipFileNames (remote)
		
			// pull and aggregrate those files into local file (F) on disk
		
			// initialise an output table of K - <V1, V2>
		
			// loop till F returns null
				// for every KV pair
		
				// call the reduce method , getting an op key and op value
		
				// add this op KV pair to the output table, appending to the value in table
		
			// sort the entries of output table by key, use sort()
		
			// Write table to op file on local disk
		
			// notify namenode to add this file to dfs
		
			// Indicate that it is finished to JobTracker (JTMonitor)
		
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void shuffle(){
		
	}
	
	// sort the keys within each partition before feeding into reducer (to be called by reducer)
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
