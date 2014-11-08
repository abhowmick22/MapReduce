package mapred;

import mapred.interfaces.Combiner;
import mapred.interfaces.Mapper;
import mapred.interfaces.Reducer;

/*
 * A user supplies an object of this class, i.e. a mapreduce job to be run on the cluster
 * Eg, distributed grep
 * 
 * Check interface type (mapper or reducer or combiner) and see if it is consistent with the
 * corresponding function signature 
 * 
 * mapper, reducer and combiner are implemeneted as interfaces instead of classes as we 
 * want them to have virtual methods (eg. map for Mapper)
 */

public class MapReduceJob{
	
	// configs for the job
	private JobConf configs;
	// the mapper class
	private Class<? extends Mapper> mapper;
	// the reducer class
	private Class<? extends Reducer> reducer;
	// the combiner class
	private Class<? extends Combiner> combiner;
	
	// set the configs for this job
	public void setConfigs(JobConf conf){
		this.configs = conf;
	}
	
	// set the mapper class for this job
	public void setMapper(Class<? extends Mapper> mapper){
		this.mapper = mapper;
	}
	
	// set the mapper class for this job
	public void setReducer(Class<? extends Reducer> reducer){
		this.reducer = reducer;
	}

	// set the combiner class for this job
	public void setCombiner(Class<? extends Combiner> combiner){
		this.combiner = combiner;
	}
	
}
