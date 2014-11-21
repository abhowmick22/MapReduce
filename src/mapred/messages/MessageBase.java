package mapred.messages;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
	
	public MessageBase(){
		try {
			this.sourceAddr = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("Message can't get its source address.");
		}
	}
	
	public String getSourceAddr(){
		return this.sourceAddr;
	}

	public String getDestAddr(){
		return this.destAddr;
	}
	
	public void setSourceAddr(String sourceAddr){
		this.sourceAddr = sourceAddr;
	}
	
	public void setDestAddr(String destAddr){
		this.destAddr = destAddr;
	}
}
