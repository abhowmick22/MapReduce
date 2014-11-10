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
	// the input files are local for map tasks
	// and remote for reduce tasks
	private List<String> ipFiles;
	
	public void setJobStopId(int jobStopId){
		this.jobStopId = jobStopId;
	}
	
	public void setMsgType(String msgType){
		this.msgType = msgType;
	}
	
	public int getJobStopId(){
		return this.jobStopId;
	}
	
	public String getMsgType(){
		return this.msgType;
	}

}
