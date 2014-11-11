package mapred.messages;

import mapred.HealthReport;

/*
 * This message is used by slave to indicate 3 things to master:
 * 1. Accept requested task
 * 2. Reject requested task
 * 3. Task finished
 * 4. Heartbeat
 * 
 * For types 1 & 2, send to port 10000
 * For types 3 & 4, send to port 10002
 */

public class SlaveToMasterMsg {
	
	// type of message
	private String type;
	// Health report in case of heartbeat message
	private HealthReport healthReport;
	
	public void setType(String type){
		this.type = type;
	}
	
	public void setHealthReport(HealthReport healthReport){
		this.healthReport = healthReport;
	}
	
	public String getType(){
		return this.type;
	}
	
	public HealthReport getHealthReport(){
		return this.healthReport;
	}

}
