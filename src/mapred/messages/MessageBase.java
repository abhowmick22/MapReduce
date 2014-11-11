package mapred.messages;

import java.io.Serializable;

/*
 * This is the base class for communication messages
 * among the mapreduce nodes
 */

public class MessageBase implements Serializable{
	
	// source address
	private String sourceAddr;
	// destination address
	private String destAddr;
	
	public String getSourceAddr(){
		return this.sourceAddr;
	}

	public String getDestAddr(){
		return this.destAddr;
	}
}
