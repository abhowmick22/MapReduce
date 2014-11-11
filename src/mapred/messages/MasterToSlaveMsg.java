package mapred.messages;

import java.util.List;

import mapred.MapReduceJob;

/*
 * This message is used by master to command slave to
 * start/stop a job
 */

public class MasterToSlaveMsg extends MessageBase {
	// the message type
	private String msgType;
	// the job to terminate
	private int jobStopId;
	// the mapreduce job for which task is to be launched
	private MapReduceJob job;
	// the type of task that is to be executed
	private String taskType;
	// list of input file names for the task
	// the input files are local for map tasks (just 1 item)
	// and remote for reduce tasks (multiple items)
	private List<String> ipFiles;
	// record numbers to read in case of map task
	private int readRecordStart;
	private int readRecordEnd;
	
	
	// Setters
	public void setJobStopId(int jobStopId){
		this.jobStopId = jobStopId;
	}
	
	public void setMsgType(String msgType){
		this.msgType = msgType;
	}
	
	public void setJob(MapReduceJob job){
		this.job = job;
	}
	
	public void setTaskType(String taskType){
		this.taskType = taskType;
	}
	
	public void setIpFiles(List<String> ipFiles){
		this.ipFiles = ipFiles;
	}
	
	public void setReadRecordStart(int readRecordStart){
		this.readRecordStart = readRecordStart;
	}
	
	public void setReadRecordEnd(int readRecordEnd){
		this.readRecordEnd = readRecordEnd;
	}
	
	
	// Getters
	public int getJobStopId(){
		return this.jobStopId;
	}
	
	public String getMsgType(){
		return this.msgType;
	}
	
	public MapReduceJob getJob(){
		return this.job;
	}
	
	public String getTaskType(){
		return this.taskType;
	}
	
	public List<String> getIpFiles(){
		return this.ipFiles;
	}
	
	public int getReadRecordStart(){
		return this.readRecordStart;
	}
	
	public int getReadRecordEnd(){
		return this.readRecordEnd;
	}

}
