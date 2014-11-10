package mapred.tests;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import mapred.MapReduceJob;
import mapred.messages.ClientAPIMsg;

/*
 * This is a test bench to test our job tracker
 */

public class DummyClientAPI {
	/***********************************************/
	public static void main(String[] args){
		
		ServerSocket JTsocket = null;
		Socket requestSocket = null;
		ObjectOutputStream requestStream = null;
		
		// Open up communications 
		try {		
			// open a new server socket for response from JobTracker
			JTsocket = new ServerSocket(20001);
			// a client socket for sending messages
			// to a JobTracker on the same machine
			requestSocket = new Socket(InetAddress.getLocalHost(), 20000);
			requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
		// Test 1. Send an empty launch request to JobTracker
		try {
			MapReduceJob job = new MapReduceJob();
			job.setJobId(145);
			ClientAPIMsg launchReq = new ClientAPIMsg();
			launchReq.setCommand("launchJob");
			launchReq.setJob(job);
			requestStream.writeObject(launchReq);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	/***********************************************/

}
