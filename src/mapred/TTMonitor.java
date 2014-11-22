package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;

/*
 * A runnable object of this type runs on the datanode as a daemon
 * It periodically checks the status of all the computing tasks running on this node (and also 
 * collects information about the state of the file system data), compiles everything into a health report
 * and sends it to the JobTracker. Use Timer for this.
 * 
 * It also collects finish messages from Task threads and forwards them to JTMonitor
 */

public class TTMonitor implements Runnable {
	
	// reference to mapredJobs of TaskTracker
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// reference to the runningTasks of TaskTracker
	private static ConcurrentHashMap<String, Task> runningTasks;
	// server socket to get messages from tasks
	private static ServerSocket msgSocket;
	// IP addr of JTMonitor to which we need to send messages
	private static String jobtrackerIpAddr;
	// port of JTMonitor to which we need to send messages
	private static int jobTrackerPort;

	// Special constructor
	public TTMonitor(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
			ConcurrentHashMap<String, Task> runningTasks, String jobtrackerIpAddr, int monitorPort,
			int jobTrackerPort){
		
		try {
			TTMonitor.mapredJobs = mapredJobs;
			TTMonitor.runningTasks = runningTasks;
			TTMonitor.jobtrackerIpAddr = jobtrackerIpAddr;
			TTMonitor.msgSocket = new ServerSocket(monitorPort);
			TTMonitor.jobTrackerPort = jobTrackerPort;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
			
			try {
				
				// start listening	
				while(true){
					Socket taskSocket = msgSocket.accept();

					ObjectInputStream taskStream = new ObjectInputStream(taskSocket.getInputStream());
					SlaveToMasterMsg taskMsg = (SlaveToMasterMsg) taskStream.readObject();
					taskStream.close();
					taskSocket.close();
					
					// extract relevant info and update tables
					String msgType = taskMsg.getMsgType();
					// ensure msgType is "finish"
					if(msgType.equals("finished")){
						int jobId = taskMsg.getFinishedTask().getFirst();
						int taskId = taskMsg.getFinishedTask().getSecond();
						if(taskMsg.getTaskType().equals("map")){
							mapredJobs.get(jobId).getMapTasks().get(taskId).setStatus("done");
							mapredJobs.get(jobId).getMapTasks().get(taskId).setOpFileNames(taskMsg.getOpFiles());
						}
						else{
							mapredJobs.get(jobId).getReduceTasks().get(taskId).setStatus("done");
						}
						// Update runningTasks
						runningTasks.remove(String.valueOf(jobId) + "-" + String.valueOf(taskId));
					}
					
					// relay the message to JTMonitor
					Socket masterSocket = new Socket(jobtrackerIpAddr, jobTrackerPort);
					ObjectOutputStream masterStream = new ObjectOutputStream(masterSocket.getOutputStream());
					masterStream.writeObject(taskMsg);
					masterStream.close();
					masterSocket.close();
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.out.println("TTMonitor can't find message class sent by execution thread");
				e.printStackTrace();
			}

	}

}
