package mapred.messages;

/*
 * This message is used by master to command slave to
 * start/stop a job
 */

public class MasterToSlaveMsg extends MessageBase {
	// the message type
	private String type;
	// the job to terminate
	private int jobStopId;
	
	public void setJobStopId(int jobStopId){
		this.jobStopId = jobStopId;
	}
	
	public void setType(String type){
		this.type = type;
	}
	
	public int getJobStopId(){
		return this.jobStopId;
	}
	
	public String getType(){
		return this.type;
	}

}
