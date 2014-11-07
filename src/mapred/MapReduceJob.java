package mapred;

/*
 * A user supplies an object of this class, i.e. a mapreduce job to be run on the cluster
 * Eg, distributed grep
 * 
 * Check interface type (mapper or reducer or combiner) and see if it is consistent with the
 * corresponding function signature 
 */

public class MapReduceJob {
	
	// input location for this job (on DFS)
	private String ipLocation;
	// output location for this job (on DFS)
	private String opLocation;
	
	
	// the map method
	public void map(String ipKey, String ipValue, String opKey, String opValue){
		
	}

	// the reduce method
	public void reduce(String ipKey, String ipValue, String opKey, String opValue){
		
	}

}
