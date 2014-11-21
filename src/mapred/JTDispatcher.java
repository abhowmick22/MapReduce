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
	private static ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	// Last scheduled job, for Round Robin Scheduler
	private static int lastScheduledJob;
	// Last scheduled task, for above job
	private static int lastScheduledTask;
	// The IP Addr of the namenode
	private static String nameNode;
	
	public JTDispatcher(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
				ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
				String nameNode){
		JTDispatcher.mapredJobs = mapredJobs;
		JTDispatcher.clusterNodes = clusterNodes;
		JTDispatcher.lastScheduledJob = 0;
		JTDispatcher.lastScheduledTask = 0;
		JTDispatcher.nameNode = nameNode;
	}

	@Override
	public void run() {
		
		try {
			JTDispatcher.ackSocket = new ServerSocket(10000);
		} catch (IOException e) {
			System.out.println("Socket for receiving ack by dispatcher already in use.");
		}
		
		// Set up the simple scheduler
		JTDispatcher.scheduler = new SimpleScheduler(JTDispatcher.mapredJobs, 
				JTDispatcher.clusterNodes, JTDispatcher.nameNode);
		
		
		while(true){
			JobTableEntry nextJob = null;
			TaskTableEntry nextTask = null;
			
			// Use the simple scheduler
			((SimpleScheduler) JTDispatcher.scheduler).setLastScheduledJob(JTDispatcher.lastScheduledJob);
			((SimpleScheduler) JTDispatcher.scheduler).setLastScheduledTask(JTDispatcher.lastScheduledTask);
			Pair<JobTableEntry, TaskTableEntry> next = JTDispatcher.scheduler.schedule();
			if(next != null){
				nextJob = next.getFirst();
				nextTask = next.getSecond();
			}
			
			if(nextTask != null && nextJob != null){
				dispatchTask(nextJob, nextTask, nextTask.getTaskType());
				
				// Do ack routines here
				try {
					Socket slaveAckSocket = ackSocket.accept();
					ObjectInputStream slaveAckStream = new ObjectInputStream(slaveAckSocket.getInputStream());
					SlaveToMasterMsg ack = (SlaveToMasterMsg) slaveAckStream.readObject();
					slaveAckStream.close();
					slaveAckSocket.close();
					
					// process ACK
					if(ack.getMsgType().equals("accept") && nextTask.getTaskType().equals("map")){
						nextJob.setStatus("map");
						nextJob.incPendingMaps();
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
						nextJob.setStatus("reduce");
						nextJob.incPendingReduces();
					}
					
				} catch (IOException e) {
					System.out.println("Dispatcher can't get connection to slave for ack.");
				} catch (ClassNotFoundException e) {
					System.out.println("Dispatcher can't find file for received ack message.");
				}
				
				nextTask.setStatus("running");
				String nodeId = nextTask.getCurrNodeId();
				Integer currLoad = JTDispatcher.clusterNodes.get(nodeId).getSecond();
				JTDispatcher.clusterNodes.get(nodeId).setSecond(currLoad + 1);
				JTDispatcher.lastScheduledJob = nextJob.getJob().getJobId();
				JTDispatcher.lastScheduledTask = nextTask.getTaskId();
			}

		}
	}
	
	// helper function to send launch messages to all (map/reduce) tasks
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
				
				// Use scheduler to get best map node to dispatch to, maybe using locality information
				nodeId = JTDispatcher.scheduler.getBestMapLocation();
			
				// Set read range for this map task
				int readRecordStart =
						nextTask.getRecordRange().getFirst();
				int readRecordEnd =
						nextTask.getRecordRange().getSecond();
				message.setReadRecordStart(readRecordStart);
				message.setReadRecordEnd(readRecordEnd);
			}
			else{	
				// Use scheduler to get best node to dispatch to, maybe using locality information
				nodeId = JTDispatcher.scheduler.getBestMapLocation();
				
				// accumulate all input files
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
			System.out.println("Dispatcher couldn't get localhost address for target node.");
		} catch (IOException e) {
			System.out.println("Dispatcher couldn't get connection to target node.");
		}
	
	}
	
	// Pretty printing of the state of cluster, for DEBUG purposes
	/*
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
	*/
}
