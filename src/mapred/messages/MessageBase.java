package mapred.messages;

import java.io.Serializable;

/*
 * This is the base class for communication messages
 * among the mapreduce nodes
 */

public class MessageBase implements Serializable{
	
	/**
	 * Serial version UID, Lulz
	 */
	private static final long serialVersionUID = -1028567408455932957L;
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
