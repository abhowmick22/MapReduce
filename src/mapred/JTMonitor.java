package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;

/*
 * A Runnable object of this type runs on the namenode as a daemon
 * It contains a serversocket to listen to health reports from the task trackers
 * 
 * Create a constructor passing in objects from JobTracker that this monitor 
 * thread would like to access on receiving messages
 * 
 * It also accumulates task finish messages. On receiving such messages from 
 * slave node, it will append the output file info into the corresponding field
 * of the reduce task.
 * The format of the output file info is assumed to be <R:nodename:filepath>
 * where R = reducer number (starting from 1), nodename is name of machine,
 * filepath, is the path on that machine 
 */

public class JTMonitor implements Runnable{
	
	// handle to the table of mapreduce jobs at JobTracker
	private ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from slaves
	private ServerSocket clientAPISocket;
	// Handle to the clusterLoad data structure of JobTracker
	private ConcurrentHashMap<String, Integer> clusterLoad;
	
	// Special constructor
	public JTMonitor(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
						ConcurrentHashMap<String, Integer> clusterLoad){
		this.mapredJobs = mapredJobs;
		this.clusterLoad = clusterLoad;
	}

	@Override
	public void run() {
		
		try {
			// Initialise the server socket to get messages from slaves
			this.clientAPISocket = new ServerSocket(10002);
			
			// start listening for messages
			while(true){
				System.out.println("JTMonitor listening...");
				Socket slaveSocket = this.clientAPISocket.accept();
				ObjectInputStream slaveStream = new ObjectInputStream(slaveSocket.getInputStream());
				SlaveToMasterMsg slaveMessage = (SlaveToMasterMsg) slaveStream.readObject();
				
				// check if the message is a task finish indicator
				if(slaveMessage.getMsgType().equals("finished")){
					int finishedJobId = slaveMessage.getFinishedTask().getFirst();
					int finishedTaskId = slaveMessage.getFinishedTask().getSecond();
					String node = slaveMessage.getSourceAddr();
					
					System.out.println("task finish message received : " + slaveMessage.getTaskType());
					
					// if map
					if(slaveMessage.getTaskType().equals("map")){
						mapredJobs.get(finishedJobId).getMapTasks().get(finishedTaskId).setStatus("done");
						mapredJobs.get(finishedJobId).getMapTasks().get(finishedTaskId).setOpFileNames(slaveMessage.getOpFiles());
					}
					// if reduce
					else{
						mapredJobs.get(finishedJobId).getReduceTasks().get(finishedTaskId).setStatus("done");
					}
					
					// update clusterLoad info
					String nodeId = mapredJobs.get(finishedJobId).getMapTasks().get(finishedTaskId).getCurrNodeId();
					Integer currLoad = clusterLoad.get(nodeId);
					clusterLoad.put(nodeId, currLoad - 1);
				}
				
				// else if message is a health monitor
				else{
					// run fault tolerance and health routines
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
