package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import mapred.interfaces.Scheduler;
import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;

/*
 * This object is responsible for continuously seeking out new tasks
 * to schedule and dispatching them on the cluster
 */

public class JTDispatcher implements Runnable {
	
	// Scheduler for allotting jobs on the slave nodes
	private Scheduler scheduler;
	// handle to the jobtracker's mapredJobs
	private ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// Socket to dispatch tasks
	private Socket dispatchSocket;
	// Socket to receive ACK/NACK
	private ServerSocket ackSocket;
	// Data Structure to store workload on each node
	private ConcurrentHashMap<String, Integer> clusterLoad;
	// Last scheduled job, for Round Robin Scheduler
	private int lastScheduledJob;
	// Last scheduled task, for above job
	private int lastScheduledTask;
	
	public JTDispatcher(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
							ConcurrentHashMap<String, Integer> clusterLoad){
		this.mapredJobs = mapredJobs;
		this.clusterLoad = clusterLoad;
		this.lastScheduledJob = 0;
		this.lastScheduledTask = 0;
	}

	@Override
	public void run() {
		
		// Set up the simple scheduler
		this.scheduler = new SimpleScheduler();
		int nextJob = 0;
		int nextTask = 0;
		String nodeId = null;
		String nextTaskType = null;
		
		while(true){
			// Use the simple scheduler
			((SimpleScheduler) this.scheduler).setLastScheduledJob(this.lastScheduledJob);
			((SimpleScheduler) this.scheduler).setLastScheduledTask(this.lastScheduledTask);
			this.scheduler.schedule(nextJob, nextTask, nodeId, nextTaskType);
			// the parameters lastScheduledJob and lastScheduledTask should not be changed by scheduler
			// check for this, primitive paramaters should be pass-by-value
			
			// Extract the correct task and job
			MapReduceJob job = this.mapredJobs.get(nextJob).getJob();
			// Prepare message and dispatch
			dispatchTask(job, nextTask, nodeId, nextTaskType);
			

		}
	}
	
	// helper function to send launch messages to all (map/reduce) tasks
	@SuppressWarnings("null")
	private void dispatchTask(MapReduceJob job, int nextTask, String nodeId, String nextTaskType){
			
		try {
			this.ackSocket = new ServerSocket(10000);
			List<String> ipFiles = null;
			Socket slaveSocket = new Socket(nodeId, 10001);
			MasterToSlaveMsg message = new MasterToSlaveMsg();
			message.setMsgType("start");
			message.setJob(job);
			message.setTaskType(nextTaskType);
			
			// get the input files
			if(nextTaskType.equals("map")){
				ipFiles.add(job.getIpFileName());
				message.setIpFiles(ipFiles);
				int readRecordStart =
						this.mapredJobs.get(job.getJobId()).getMapTasks().get(nextTask).getRecordRange().get(0);
				int readRecordEnd =
						this.mapredJobs.get(job.getJobId()).getMapTasks().get(nextTask).getRecordRange().get(1);
				message.setReadRecordStart(readRecordStart);
				message.setReadRecordEnd(readRecordEnd);
			}
			// TODO : Implement getting files for reduce
			else{
				
			}
			
			ObjectOutputStream slaveStream = new ObjectOutputStream(slaveSocket.getOutputStream());
			slaveStream.writeObject(message);
			slaveStream.close();
			slaveSocket.close();
			
			// wait for ACK
			Socket slaveAckSocket = ackSocket.accept();
			ObjectInputStream slaveAckStream = new ObjectInputStream(slaveSocket.getInputStream());
			SlaveToMasterMsg ack = (SlaveToMasterMsg) slaveAckStream.readObject();
			slaveAckStream.close();
			slaveAckSocket.close();
			
			// process ACK
			if(ack.getType().equals("accept") && nextTaskType.equals("map")){
				this.mapredJobs.get(job.getJobId()).setStatus("map");
				this.mapredJobs.get(job.getJobId()).getMapTasks().get(nextTask).setStatus("running");
				this.mapredJobs.get(job.getJobId()).getMapTasks().get(nextTask).setCurrNodeId(nodeId);
				Integer currLoad = this.clusterLoad.get(nodeId);
				this.clusterLoad.put(nodeId, currLoad + 1);
				this.lastScheduledJob = job.getJobId();
				this.lastScheduledTask = nextTask;
			}
			else if(ack.getType().equals("accept") && nextTaskType.equals("reduce")){
				
			}
			else if(ack.getType().equals("reject") && nextTaskType.equals("map")){
				
			}
			else{
			// (ack.getType().equals("reject") && nextTaskType.equals("reduce"))
				
			}
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}
