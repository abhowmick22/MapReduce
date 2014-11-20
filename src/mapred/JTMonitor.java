package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
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
	private ConcurrentHashMap<String, Integer> clusterLoad;
	
	// Special constructor
	public JTMonitor(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
						ConcurrentHashMap<String, Integer> clusterLoad,
						ServerSocket monitorSocket){
		this.mapredJobs = mapredJobs;
		this.clusterLoad = clusterLoad;
		this.monitorSocket = monitorSocket;
	}

	@Override
	public void run() {
		
		try {
			// Initialise the server socket to get messages from slaves
			//this.monitorSocket = new ServerSocket(10002);
			
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
					
					System.out.println("task finish message received : " + slaveMessage.getTaskType());
					
					if(slaveMessage.getTaskType().equals("map")){	
						finishedTask.setStatus("done");
						finishedTask.setOpFileNames(slaveMessage.getOpFiles());
						finishedJob.decPendingMaps();

						if(mapredJobs.get(finishedJobId).getPendingMaps() == 0)
							finishedJob.setStatus("reduce");
									
					}
					else{	
						finishedTask.setStatus("done");
						finishedJob.decPendingReduces();
						
						if(finishedJob.getPendingReduces() == 0)
							finishedJob.setStatus("done");
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
