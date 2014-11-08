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
	String filepath;
	// The job of which this task is part
	MapReduceJob parentJob;
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
