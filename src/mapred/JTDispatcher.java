package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
import mapred.types.MapReduceJob;
import mapred.types.TaskTableEntry;

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
		JobTableEntry nextJob = null;
		TaskTableEntry nextTask = null;
		
		while(true){
			// Use the simple scheduler
			((SimpleScheduler) this.scheduler).setLastScheduledJob(this.lastScheduledJob);
			((SimpleScheduler) this.scheduler).setLastScheduledTask(this.lastScheduledTask);
			this.scheduler.schedule(nextJob, nextTask);
			
			if(nextTask != null){
				dispatchTask(nextJob, nextTask, nextTask.getTaskType());	
				System.out.println("JTDispatcher dispatched job: " + nextJob.getJob().getJobId()
									+ " task: " + nextTask.getTaskId());
			}
		}
	}
	
	// helper function to send launch messages to all (map/reduce) tasks
	@SuppressWarnings("null")
	private void dispatchTask(JobTableEntry job, TaskTableEntry nextTask, String nextTaskType){
			
		try {

			List<String> ipFiles = null;
			MasterToSlaveMsg message = new MasterToSlaveMsg();
			String nodeId = null;
			int nextTaskId = nextTask.getTaskId();
			
			// get the input files and node Id for the task
			if(nextTaskType.equals("map")){
				
				// TODO: finalise the logic to calculate fileBlockName which this mapper takes
				String fileBlockName = job.getJob().getIpFileName() + "-" 
										+ Integer.toString(job.getJob().getIpFileSize()/job.getJob().getBlockSize());
				ipFiles.add(fileBlockName);
				
				// TODO: figure out the nodeId to which this mapper should go by supplying fileBlockName to namenode
				nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
			
				// Set read range for this map task
				int readRecordStart =
						nextTask.getRecordRange().get(0);
				int readRecordEnd =
						nextTask.getRecordRange().get(1);
				message.setReadRecordStart(readRecordStart);
				message.setReadRecordEnd(readRecordEnd);
			}
			else{	
				// TODO: figure out which nodeId to dispatch reduce task to
				nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
				
				// accumulate all input files and node id
				nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = job.getMapTasks();
				ConcurrentHashMap<Integer, String> opFiles = null;
				Integer partitionNum;
				for(TaskTableEntry entry : tasks.values()){
					opFiles = entry.getOpFileNames();
					for(String file : opFiles.values()){
						String[] parts = file.split("-");
						partitionNum = Integer.valueOf(parts[1]);
						if(partitionNum.equals(nextTaskId)){	// assuming task id is equal to partition number
							ipFiles.add(parts[0]);
						}
					}
				}
				
			}
			
			this.ackSocket = new ServerSocket(10000);
			this.dispatchSocket = new Socket(nodeId, 10001);
			message.setIpFiles(ipFiles);
			message.setMsgType("start");
			message.setJob(job.getJob());
			message.setTaskType(nextTaskType);
			message.setTaskId(nextTaskId);
			
			ObjectOutputStream dispatchStream = new ObjectOutputStream(this.dispatchSocket.getOutputStream());
			dispatchStream.writeObject(message);
			dispatchStream.close();
			this.dispatchSocket.close();
			
			// wait for ACK
			Socket slaveAckSocket = ackSocket.accept();
			ObjectInputStream slaveAckStream = new ObjectInputStream(slaveAckSocket.getInputStream());
			SlaveToMasterMsg ack = (SlaveToMasterMsg) slaveAckStream.readObject();
			slaveAckStream.close();
			slaveAckSocket.close();
			
			// process ACK
			if(ack.getMsgType().equals("accept") && nextTaskType.equals("map")){
				job.setStatus("map");
				job.incPendingMaps();
			}
			else if(ack.getMsgType().equals("accept") && nextTaskType.equals("reduce")){
				job.setStatus("reduce");
				job.incPendingReduces();
			}
			else if(ack.getMsgType().equals("reject") && nextTaskType.equals("map")){
				job.setStatus("map");
				job.incPendingMaps();
			}
			else{
			// (ack.getType().equals("reject") && nextTaskType.equals("reduce"))
				job.setStatus("reduce");
				job.incPendingReduces();
			}
			
			nextTask.setStatus("running");
			nextTask.setCurrNodeId(nodeId);
			Integer currLoad = this.clusterLoad.get(nodeId);
			this.clusterLoad.put(nodeId, currLoad + 1);
			this.lastScheduledJob = job.getJob().getJobId();
			this.lastScheduledTask = nextTaskId;
			
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
