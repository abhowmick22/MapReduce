package mapred.messages;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import mapred.types.HealthReport;
import mapred.types.Pair;



/*
 * This message is used by slave to indicate 3 things to master:
 * 1. Accept requested task
 * 2. Reject requested task
 * 3. Task finished
 * 4. Heartbeat
 * 
 * For types 1 & 2, send to port 10000	- JobTracker
 * For types 3 & 4, send to port 10002
 */

public class SlaveToMasterMsg extends MessageBase{
	
	// type of message
	private String msgType;
	// Health report in case of heartbeat message
	private HealthReport healthReport;
	// In case of task finished, Pair of (JobId, TaskId) is sent
	private Pair<Integer> finishedTask;
	// type of task finished
	private String taskType;
	// In case of map task finished, send a list of paths of output files, stored in dfs
	private ConcurrentHashMap<Integer, String> opFiles;
	
	public SlaveToMasterMsg(){
		try {
			this.setSourceAddr(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setFinishedTask(Pair<Integer> finishedTask){
		this.finishedTask = finishedTask;
	}
	
	public void setOpFiles(ConcurrentHashMap<Integer, String> opFiles){
		this.opFiles = opFiles;
	}
	
	public void setMsgType(String type){
		this.msgType = type;
	}
	
	public void setTaskType(String type){
		this.taskType = type;
	}
	
	public void setHealthReport(HealthReport healthReport){
		this.healthReport = healthReport;
	}
	
	public Pair<Integer> getFinishedTask(){
		return this.finishedTask;
	}
	
	public ConcurrentHashMap<Integer, String> getOpFiles(){
		return this.opFiles;
	}
	
	public String getMsgType(){
		return this.msgType;
	}
	
	public String getTaskType(){
		return this.taskType;
	}
	
	public HealthReport getHealthReport(){
		return this.healthReport;
	}

}
