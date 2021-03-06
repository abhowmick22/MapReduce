package mapred.types;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import dfs.InputSplit;
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
	
	/**
	 * SerialVersionUID lulz
	 */
	private static final long serialVersionUID = 1L;
	// Name of the job if you feel like it
	private String jobName;
	// input file name for this job (on DFS)
	private String ipFileName;
	// output file name for this job (on DFS)
	private String opFileName;
	// unique Id for the job
	private int jobId;
	// indicator if combiner class exists
	private boolean ifCombiner;
	// username of the user who launched the job
	private String userName;
	// jarpath
	private String jarPath;
	
	/* 
	 * If you define a reference variable whose type is an interface, 
	 * any object you assign to it must be an instance of a class that implements the interface.
	*/
	// the name of the mapper class
	private String mapperClass;
	// the name of the reducer class
	private String reducerClass;
	// the combiner class
	private Combiner combiner;
	// file block size
	private int blockSize;
	// split size of file block in bytes
	private int splitSize;
	// file size
	private int ipFileSize;
	// Number of reducers
	private int numReducers;
	// the size of the record, to be read from config file
	private int recordSize;
	//input split
	private InputSplit inputSplit;
	
	// Constructor
	public MapReduceJob(){
		this.splitSize = 120;				// default size of split mapper will work on
		this.recordSize = 60;				// default record size in bytes
		this.ifCombiner = false;
		
		// default number of mappers and reducers
		this.numReducers = 1;
		try {
			this.userName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// set inputSplit
    public void setInputSplit(InputSplit inputSplit){
        this.inputSplit = inputSplit;
    }
	
	// set jarPath
	public void setJarPath(String jarPath){
		this.jarPath = "/tmp/" + this.userName + "/" + jarPath;
	}
	
	
	// set recordSize
	public void setRecordSize(int recordSize){
		this.recordSize = recordSize;
	}
	
	// set userName
	public void setUserName(String userName){
		this.userName = userName;
	}
	
	// set ifCombiner
	public void setIfCombiner(boolean ifCombiner){
		this.ifCombiner = ifCombiner;
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
	
	// set the blockSize
	public void setBlockSize(int blockSize){
		this.blockSize = blockSize;
	}
	
	// set number of reducers
	public void setNumReducers(int numReducers){
		this.numReducers = numReducers;
	}
	
	// set the mapper class for this job
	public void setMapper(String mapperClass){
		this.mapperClass = mapperClass;
	}
	
	// set the reducer class for this job
	public void setReducer(String reducerClass){
		this.reducerClass = reducerClass;
	}

	// set the combiner class for this job
	public void setCombiner(Combiner combiner){
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
	
	// get inputSplit
    public InputSplit getInputSplit(){
        return this.inputSplit;
    }
    
	// get recordSize
	public int getRecordSize(){
		return this.recordSize;
	}
	
	// get userName
	public String getUserName(){
		return this.userName;
	}
	
	// get ifCombiner
	public boolean getIfCombiner(){
		return this.ifCombiner;
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
	
	// get the blockSize
	public int getBlockSize(){
		return this.blockSize;
	}
	
	// get number of reducers
	public int getNumReducers(){
		return this.numReducers;
	}

	// get the mapper class for this job
	public String getMapper(){
		return this.mapperClass;
	}
	
	// get the reducer class for this job
	public String getReducer(){
		return this.reducerClass;
	}

	// get the combiner class for this job
	public Combiner getCombiner(){
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
	
	public String getJarPath(){
		return this.jarPath;
	}
	
}
