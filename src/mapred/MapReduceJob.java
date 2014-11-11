package mapred;

import java.io.Serializable;

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

public class MapReduceJob implements Serializable{
	
	// Name of the job if you feel ike it
	private String jobName;
	// input file name for this job (on DFS)
	private String ipFileName;
	// output file name for this job (on DFS)
	private String opFileName;
	// unique Id for the job
	private int jobId;
	// the mapper class
	private Class<? extends Mapper> mapper;
	// the reducer class
	private Class<? extends Reducer> reducer;
	// the combiner class
	private Class<? extends Combiner> combiner;
	// file block size
	private int splitSize;
	// file size
	private int ipFileSize;
	// Number of mappers = ipFileSize / splitSize ??
	private int numMappers;
	// Number of reducers
	private int numReducers;
	// Number of partitions == reducers ??
	private int numPartitions;
	// Threshold on number of intermediate pairs after which output of map is flushed to disk
	private int spillThreshold;
	
	// Constructor
	public MapReduceJob(){
		this.splitSize = 100;				// default number of records
	}
	
	// set the jobName
	public void setJobName(String name){
		this.jobName = name;
	}
	
	// set the ipFileSize
	public void setIpFileSize(int ipFileSize){
		this.ipFileSize = ipFileSize;
	}
	
	// set the splitSize
	public void setSplitSize(int splitSize){
		this.splitSize = splitSize;
	}
	
	// set number of reducers
	public void setNumReducers(int numReducers){
		this.numReducers = numReducers;
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
	
	// set the job id for this job
	public void setJobId(int jobId){
		this.jobId = jobId;
	}
	
	public void setIpFileName(String ipFileName){
		this.ipFileName = ipFileName;
	}
	
	public void setOpFileName(String opFileName){
		this.opFileName = opFileName;
	}
	
	// get the jobName
	public String getJobName(){
		return this.jobName;
	}
	
	// get the ipFileSize
	public int getIpFileSize(){
		return this.ipFileSize;
	}
	
	// get the splitSize
	public int getSplitSize(){
		return this.splitSize;
	}
	
	// get number of reducers
	public int getNumReducers(){
		return this.numReducers;
	}

	// get the mapper class for this job
	public Class<? extends Mapper> getMapper(){
		return this.mapper;
	}
	
	// get the mapper class for this job
	public Class<? extends Reducer> getReducer(){
		return this.reducer;
	}

	// get the combiner class for this job
	public Class<? extends Combiner> getCombiner(){
		return this.combiner;
	}
	
	// get the job id for this job
	public int getJobId(){
		return this.jobId;
	}
	
	public String getIpFileName(){
		return this.ipFileName;
	}
	
	public String getOpFileName(){
		return this.opFileName;
	}
	
}
