package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.messages.MasterToSlaveMsg;
import mapred.messages.SlaveToMasterMsg;
import mapred.types.JobTableEntry;
import mapred.types.MapReduceJob;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

/*
 * This object is responsible for continuously seeking out new tasks
 * to schedule and dispatching them on the cluster
 */

public class JTDispatcher implements Runnable {
	
	// Scheduler for allotting jobs on the slave nodes
	private static Scheduler scheduler;
	// handle to the jobtracker's mapredJobs
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// Socket to dispatch tasks
	private static Socket dispatchSocket;
	// Socket to receive ACK/NACK
	private static ServerSocket ackSocket;
	// Data Structure to store workload on each node
	private static ConcurrentHashMap<String, Integer> clusterLoad;
	// Last scheduled job, for Round Robin Scheduler
	private static int lastScheduledJob;
	// Last scheduled task, for above job
	private static int lastScheduledTask;
	
	public JTDispatcher(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
							ConcurrentHashMap<String, Integer> clusterLoad){
		JTDispatcher.mapredJobs = mapredJobs;
		JTDispatcher.clusterLoad = clusterLoad;
		JTDispatcher.lastScheduledJob = 0;
		JTDispatcher.lastScheduledTask = 0;
	}

	@Override
	public void run() {
		
		try {
			JTDispatcher.ackSocket = new ServerSocket(10000);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Set up the simple scheduler
		JTDispatcher.scheduler = new SimpleScheduler(JTDispatcher.mapredJobs);
		
		
		while(true){
			JobTableEntry nextJob = null;
			TaskTableEntry nextTask = null;
			
			// Use the simple scheduler
			((SimpleScheduler) JTDispatcher.scheduler).setLastScheduledJob(JTDispatcher.lastScheduledJob);
			((SimpleScheduler) JTDispatcher.scheduler).setLastScheduledTask(JTDispatcher.lastScheduledTask);
			//System.out.println("JTDispatcher: Num of jobs is " + JTDispatcher.mapredJobs.size());
			//System.out.println("dispatcher: " + this.mapredJobs.size() + "-" + this.mapredJobs.get(0));
			Pair<JobTableEntry, TaskTableEntry> next = JTDispatcher.scheduler.schedule();
			if(next != null){
				nextJob = next.getFirst();
				nextTask = next.getSecond();
			}
			
			if(nextTask != null && nextJob != null){
				dispatchTask(nextJob, nextTask, nextTask.getTaskType());
				//System.out.println("JTDispatcher dispatched job: " + nextJob.getJob().getJobId()
									//+ " task: " + nextTask.getTaskId() + " of type " + nextTask.getTaskType());
				
				// Do ack routines here
				// wait for ACK if dispatch returns true
				try {
					Socket slaveAckSocket = ackSocket.accept();
					ObjectInputStream slaveAckStream = new ObjectInputStream(slaveAckSocket.getInputStream());
					SlaveToMasterMsg ack = (SlaveToMasterMsg) slaveAckStream.readObject();
					slaveAckStream.close();
					slaveAckSocket.close();
					
					//System.out.println("Ack for dispatch received");
					
					// process ACK
					if(ack.getMsgType().equals("accept") && nextTask.getTaskType().equals("map")){
						nextJob.setStatus("map");
						nextJob.incPendingMaps();
						//System.out.println("map accepted");
					}
					else if(ack.getMsgType().equals("accept") && nextTask.getTaskType().equals("reduce")){
						nextJob.setStatus("reduce");
						nextJob.incPendingReduces();
					}
					else if(ack.getMsgType().equals("reject") && nextTask.getTaskType().equals("map")){
						nextJob.setStatus("map");
						nextJob.incPendingMaps();
					}
					else{
					// (ack.getType().equals("reject") && nextTaskType.equals("reduce"))
						nextJob.setStatus("reduce");
						nextJob.incPendingReduces();
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				nextTask.setStatus("running");
				String nodeId = nextTask.getCurrNodeId();
				Integer currLoad = JTDispatcher.clusterLoad.get(nodeId);
				JTDispatcher.clusterLoad.put(nodeId, currLoad + 1);
				JTDispatcher.lastScheduledJob = nextJob.getJob().getJobId();
				JTDispatcher.lastScheduledTask = nextTask.getTaskId();
				
				//System.out.println("dispatch finished");

			}
			
			
			
		}
	}
	
	// helper function to send launch messages to all (map/reduce) tasks
	@SuppressWarnings("null")
	private void dispatchTask(JobTableEntry job, TaskTableEntry nextTask, String nextTaskType){
			
		try {

			List<String> ipFiles = new ArrayList<String>();
			MasterToSlaveMsg message = new MasterToSlaveMsg();
			String nodeId = null;
			int nextTaskId = nextTask.getTaskId();
			
			// get the input files and node Id for the task
			if(nextTaskType.equals("map")){
				
				// TODO: finalize the logic to calculate fileBlockName which this mapper takes
				//String fileBlockName = job.getJob().getIpFileName() + "-" 
				//						+ Integer.toString(job.getJob().getIpFileSize()/job.getJob().getBlockSize());
				String fileBlockName = job.getJob().getIpFileName();
				ipFiles.add(fileBlockName);
				
				// TODO: figure out the nodeId to which this mapper should go by supplying fileBlockName to namenode
				nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
			
				// Set read range for this map task
				int readRecordStart =
						nextTask.getRecordRange().getFirst();
				int readRecordEnd =
						nextTask.getRecordRange().getSecond();
				message.setReadRecordStart(readRecordStart);
				message.setReadRecordEnd(readRecordEnd);
			}
			else{	
				// TODO: figure out which nodeId to dispatch reduce task to
				nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
				
				// accumulate all input files and node id
				//nodeId = InetAddress.getLocalHost().getHostAddress();		// Placeholder for testing
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = job.getMapTasks();
				ConcurrentHashMap<Integer, String> opFiles = null;
				Integer partitionNum;
				for(TaskTableEntry entry : tasks.values()){
					opFiles = entry.getOpFileNames();
					for(String file : opFiles.values()){
						String[] parts = file.split("-");
						partitionNum = Integer.valueOf(parts[2]);
						if(partitionNum.equals(nextTaskId)){	// assuming task id is equal to partition number
							ipFiles.add(file);
						}
					}
				}
				
			}
			
			JTDispatcher.dispatchSocket = new Socket(nodeId, 10001);
			message.setIpFiles(ipFiles);
			message.setMsgType("start");
			message.setJob(job.getJob());
			message.setTaskType(nextTaskType);
			message.setTaskId(nextTaskId);
			
			ObjectOutputStream dispatchStream = new ObjectOutputStream(JTDispatcher.dispatchSocket.getOutputStream());
			dispatchStream.writeObject(message);
			dispatchStream.close();
			JTDispatcher.dispatchSocket.close();
			
			nextTask.setCurrNodeId(nodeId);
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	// Pretty printing of the state of cluster
		private static void printState(){
			// print the jobs table
			System.out.println("------------STATE OF CLUSTER------------------");
			for(JobTableEntry job : mapredJobs.values()){
				System.out.println("Job Id: " + job.getJob().getJobId() + " | Job Name: "  + job.getJob().getJobName() +
									" | Status: " + job.getStatus() +
									" | IP File Name: " + job.getJob().getIpFileName());
				System.out.println("\t\t---------Map Tasks--------------");
					for(TaskTableEntry mapTask : job.getMapTasks().values()){
						System.out.println("\t\t\tTask Id: " + mapTask.getTaskId() + " | Status: " + 
								mapTask.getStatus() + " | Node: " + mapTask.getCurrNodeId() +
								" | Start record: " + mapTask.getRecordRange().getFirst() + 
								" | End record: " + mapTask.getRecordRange().getSecond());
					}
					
			}
			System.out.println("------------END OF STATE------------------");
		}

}
