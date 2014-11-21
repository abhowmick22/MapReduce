package mapred.messages;

import java.util.HashMap;

import mapred.types.MapReduceJob;



public class ClientAPIMsg extends MessageBase{
	/**
	 * Serial UID lulz
	 */
	private static final long serialVersionUID = 1L;
	// type of message - "launchJob", "stopJob", "status", "report"
	private String command;
	// mapreduce job, if command = "launchJob"
	private MapReduceJob job;
	// jobId to stop, if command = "stopJob"
	private int jobId;
	// list of jobs to be returned running with status, if command = "status"
	// report message will have command field set to "reply"
	private HashMap<Integer, String> report;
	
	public String getCommand(){
		return this.command;
	}
	
	public MapReduceJob getJob(){
		return this.job;
	}
	
	public int getJobId(){
		return this.jobId;
	}
	
	public HashMap<Integer, String> getReport(){
		return this.report;
	}
	
	public void setCommand(String command){
		this.command = command;
	}
	
	public void setJob(MapReduceJob job){
		this.job = job;
	}
	
	public void setJobId(int jobId){
		this.jobId = jobId;
	}
	
	public void setReport(HashMap<Integer, String> report){
		this.report = report;
	}
}
