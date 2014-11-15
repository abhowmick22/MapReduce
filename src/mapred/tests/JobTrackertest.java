package mapred.tests;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import mapred.MapReduceJob;
import mapred.messages.ClientAPIMsg;

/*
 * This is a test bench to test our job tracker, behaves as a client API
 */

public class JobTrackertest {
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
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test 1. Send an empty launch request to JobTracker
		try {
			MapReduceJob job = new MapReduceJob();
			job.setIpFileName("Dummy.txt");
			job.setJobName("Distributed Dummy");
			requestSocket = new Socket(InetAddress.getLocalHost(), 20000);
			requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
			ClientAPIMsg launchReq = new ClientAPIMsg();
			launchReq.setCommand("launchJob");
			launchReq.setJob(job);
			requestStream.writeObject(launchReq);
			requestStream.close();
			requestSocket.close();
			System.out.println("Sent a launch request");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test 2. Send a status request to JobTracker	
		try {
			ClientAPIMsg statusReq = new ClientAPIMsg();
			statusReq.setCommand("status");
			requestSocket = new Socket(InetAddress.getLocalHost(), 20000);
			requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
			requestStream.writeObject(statusReq);
			requestStream.close();
			requestSocket.close();
			System.out.println("Sent a status report request");
			Socket JT = JTsocket.accept();
			ObjectInputStream JTStream = new ObjectInputStream(JT.getInputStream());
			ClientAPIMsg reply = (ClientAPIMsg) JTStream.readObject();
			HashMap<Integer, String> report = reply.getReport();
			JTStream.close();
			JT.close();
			System.out.println("System status report is: ");
			System.out.println(report);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test 3. Send a stop job request
		try {
			ClientAPIMsg stopReq = new ClientAPIMsg();
			stopReq.setCommand("stopJob");
			stopReq.setJobId(1);
			requestSocket = new Socket(InetAddress.getLocalHost(), 20000);
			requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
			requestStream.writeObject(stopReq);
			requestStream.close();
			requestSocket.close();
			System.out.println("Sent a stop request");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test 4. Send a status request to JobTracker	
		try {
			ClientAPIMsg statusReq = new ClientAPIMsg();
			statusReq.setCommand("status");
			requestSocket = new Socket(InetAddress.getLocalHost(), 20000);
			requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
			requestStream.writeObject(statusReq);
			requestStream.close();
			requestSocket.close();
			System.out.println("Sent a status report request");
			Socket JT = JTsocket.accept();
			ObjectInputStream JTStream = new ObjectInputStream(JT.getInputStream());
			ClientAPIMsg reply = (ClientAPIMsg) JTStream.readObject();
			HashMap<Integer, String> report = reply.getReport();
			JTStream.close();
			JT.close();
			System.out.println("System status report is: ");
			System.out.println(report);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	/***********************************************/

}
