package mapred;

/*
 * This object stores all the configurations for a particular job
 */

public class JobConf {
	
	// input location for this job (on DFS)
	private String ipLocation;
	// output location for this job (on DFS)
	private String opLocation;
	// file block size
	private int splitSize;
	// Number of reducers
	private int numReducers;
	// Number of partitions
	private int numPartitions;
	// Threshold on number of intermediate pairs after which output of map is flushed to disk
	private int spillThreshold;

}
