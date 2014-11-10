package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapred.interfaces.Scheduler;
import mapred.messages.ClientAPIMsg;

/*
 * Single object of this class runs on the master machine (Namenode) and controls all the TaskTracker instances on
 * the slave machines
 */

public class JobTracker{
	
	// A HashTable for maintaining the list of MapReduceJob's this is handling
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from clientAPI
	private static ServerSocket clientAPISocket;
	
	/* Configurations for the cluster */

	public static void main(String[] args) {
		
		// Do various init routines
		try {
			
			// initialize clientAPI socket
			clientAPISocket = new ServerSocket(20000);
			
			// start the jobtracker monitoring thread
			Thread monitorThread = new Thread(new JTMonitor());
			monitorThread.run();
			
			// start the jobtracker dispatcher thread
			Thread dispatcherThread = new Thread(new JTDispatcher(mapredJobs));
			dispatcherThread.run();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Start listening for mapreduce jobs from clientAPI
		while(true){
			try {
				System.out.println("Listening...");
				Socket client = clientAPISocket.accept();
				ClientAPIMsg msg = (ClientAPIMsg) new ObjectInputStream(client.getInputStream()).readObject();
				System.out.println("Read clientAPI message with request type " + msg.getCommand());
				System.out.println("Job Id of this request is " + msg.getJobId());
				Thread serviceThread = new Thread(new JTProcessRequest(msg, mapredJobs));
				serviceThread.run();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
}
