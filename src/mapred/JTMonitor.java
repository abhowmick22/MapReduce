package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

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
 * The format of the output file info is assumed to be <nodename:filepath-R>
 * where R = reducer number (starting from 1), nodename is name of machine,
 * filepath, is the path on that machine 
 */

public class JTMonitor implements Runnable{
	
	// handle to the table of mapreduce jobs at JobTracker
	private ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// server socket for listening from slaves
	private ServerSocket monitorSocket;
	// Handle to the clusterLoad data structure of JobTracker
	private ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;

	
	// Special constructor
	public JTMonitor(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
			ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
			ServerSocket monitorSocket){
		this.mapredJobs = mapredJobs;
		this.clusterNodes = clusterNodes;
		this.monitorSocket = monitorSocket;
	}

	@Override
	public void run() {
		
		try {
			
			// start listening for messages
			while(true){
				Socket slaveSocket = this.monitorSocket.accept();
				ObjectInputStream slaveStream = new ObjectInputStream(slaveSocket.getInputStream());
				SlaveToMasterMsg slaveMessage = (SlaveToMasterMsg) slaveStream.readObject();
				
				// check if the message is a task finish indicator
				if(slaveMessage.getMsgType().equals("finished")){
					int finishedJobId = slaveMessage.getFinishedTask().getFirst();
					int finishedTaskId = slaveMessage.getFinishedTask().getSecond();
					
					JobTableEntry finishedJob= mapredJobs.get(finishedJobId);		
					TaskTableEntry finishedTask = finishedJob.getMapTasks().get(finishedTaskId);
					
					finishedTask.setStatus("done");
					if(slaveMessage.getTaskType().equals("map")){	
						finishedTask.setOpFileNames(slaveMessage.getOpFiles());
						finishedJob.decPendingMaps();
						System.out.println("A map is done");

						if(finishedJob.getPendingMaps() == 0){
							System.out.println("All maps are done");
							finishedJob.setStatus("reduce");
						}
					}
					else{	
						finishedJob.decPendingReduces();
						System.out.println("A reduce is done");
						if(finishedJob.getPendingReduces() == 0){
							finishedJob.setStatus("done");
							System.out.println("Job is done");
						}
					}
					
					// update clusterLoad info
					String nodeId = mapredJobs.get(finishedJobId).getMapTasks().get(finishedTaskId).getCurrNodeId();
					Integer currLoad = clusterNodes.get(nodeId).getSecond();
					clusterNodes.get(nodeId).setSecond(currLoad - 1);
				}
				
				// else if message is a health monitor
				else{
					System.out.println("JTMonitor: Couldn't understand received message.");
				}
			}
		} catch (IOException e) {
			System.out.println("JTMonitor can't secure connection for reading message from TTMonitor.");
		} catch (ClassNotFoundException e) {
			System.out.println("JTMonitor couldn't find class for message received from slave.");
		}
		
		
	}

}
