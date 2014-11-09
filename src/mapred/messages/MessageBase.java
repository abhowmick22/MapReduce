package mapred.messages;

/*
 * This is the base class for communication messages
 * among the mapreduce nodes
 */

public class MessageBase {
	
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
