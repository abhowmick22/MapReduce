package mapred;

import java.io.Serializable;

/*
 * Objects of this instance are units of computation
 * this is run in a thread by the TaskTracker
 * 
 * Right now, we assume that a single task works on an entire file
 * chunk, there is no further splitting among partitions for tasks
 */

public class Task implements Runnable{
	
	// The file chunk which this task takes as input
	private String filepath;
	// The job of which this task is part
	private MapReduceJob parentJob;
	// Type of task
	private String taskType;
	// Task Id
	private int taskId;
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
