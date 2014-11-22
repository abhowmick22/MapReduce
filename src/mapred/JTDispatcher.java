package mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import dfs.DfsService;


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
	// map of active nodes to the tasks
	private static ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>> activeNodes;
	// port to dispatch tasks
	private static int dispatchPort;
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
	// the port of the namenode, read from config file
	private static int nameNodePort;
	private static int blockSize;
	// record size of files
	//private int recordSize;
	// split size of for mappers
	private static int splitSize;
	
	public JTDispatcher(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs,
				ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>> activeNodes,
				ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
				String nameNode, int nameNodePort, ServerSocket ackSocket, int dispatchPort,
				int blockSize, int splitSize){
		JTDispatcher.mapredJobs = mapredJobs;
		JTDispatcher.activeNodes = activeNodes;
		JTDispatcher.clusterNodes = clusterNodes;
		JTDispatcher.lastScheduledJob = 0;
		JTDispatcher.lastScheduledTask = 0;
		JTDispatcher.nameNode = nameNode;
		JTDispatcher.nameNodePort = nameNodePort;
		JTDispatcher.ackSocket = ackSocket;
		JTDispatcher.dispatchPort = dispatchPort;
		JTDispatcher.blockSize = blockSize;
		JTDispatcher.splitSize = splitSize;
	}

	@Override
	public void run() {
			
		// Set up the simple scheduler
		JTDispatcher.scheduler = new SimpleScheduler(JTDispatcher.mapredJobs, 
				JTDispatcher.clusterNodes, JTDispatcher.nameNode, JTDispatcher.nameNodePort);
		
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
				System.out.println("Dispatcher got a job to schedule.");

				if(!dispatchTask(nextJob, nextTask, nextTask.getTaskType()))
					continue;
				System.out.println("Dispatcher dispatched a task");

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
					}
					else if(ack.getMsgType().equals("accept") && nextTask.getTaskType().equals("reduce")){
						nextJob.setStatus("reduce");
					}
					//else(ack.getMsgType().equals("reject")){
					else{
						//nextJob.setStatus("map");
						continue;
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
				Pair<JobTableEntry, TaskTableEntry> pair = new Pair<JobTableEntry, TaskTableEntry>();
				pair.setFirst(nextJob);
				pair.setSecond(nextTask);
				JTDispatcher.activeNodes.get(nodeId).add(pair);
				System.out.println("task sent by Dispatcher accepted");

				/* For DEBUG
				System.out.println("\n\nactive tasks are- ");
				for(Entry<String, ArrayList<TaskTableEntry>> elem : activeNodes.entrySet()){
					int listSize = elem.getValue().size();
					if(listSize > 0){
						for(int i=0;i<listSize;i++)
						System.out.println(elem.getKey() + "\t" + elem.getValue().get(i).getCurrNodeId() + "\t" 
												+ elem.getValue().get(i).getTaskId() + "\t" 
												+  elem.getValue().get(i).getTaskType() + "\t"
												+ elem.getValue().get(i).getStatus());
					}
				}
				*/
				
				JTDispatcher.lastScheduledJob = nextJob.getJob().getJobId();
				JTDispatcher.lastScheduledTask = nextTask.getTaskId();
			}

		}
	}
	
	// helper function to send launch messages to all (map/reduce) tasks
	private boolean dispatchTask(JobTableEntry job, TaskTableEntry nextTask, String nextTaskType){
			
		try {
			List<String> ipFiles = new ArrayList<String>();
			String opFile = job.getJob().getOpFileName();
			MasterToSlaveMsg message = new MasterToSlaveMsg();
			String nodeId = null;
			int nextTaskId = nextTask.getTaskId();
			
			// get the input files and node Id for the task
			if(nextTaskType.equals("map")){
				
				// TODO: Check the logic to calculate fileBlockName which this mapper takes
				// Check the following logic
				String fileName = job.getJob().getIpFileName();	// this is the user provided dfs path
				String fileBlockName = null;	// this will be the block name that has been determined by the namenode
				// determine the block number this task should work on
				
				int numSplitsPerBlock = 1;
				if(splitSize != 0)	numSplitsPerBlock = blockSize/splitSize;
				
				// calculate the blocNumber = floor(splitNbr/numSplitsPerBlock + 1)
				int blockNumber = (int) Math.floor((nextTask.getTaskId()/numSplitsPerBlock) + 1);
				int block = 0;
				// the list of nodes across which this fileBlock is replicated
				List<String> candidateNodes = null;;		
				// Now get the map of this file to all the nodes on which its blocks reside
				Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNode, nameNodePort);
				DfsService nameNodeService = (DfsService) nameNodeRegistry.lookup("DfsService");
				//System.out.println(fileName + " " + job.getJob().getUserName());
				Map<String, List<String>> map = nameNodeService.getFileFromDfs(fileName, job.getJob().getUserName());
				//Map<String, List<String>> map = nameNodeService.getFileFromDfs(fileName, "abhishek");

				// from this map, choose the one with proper block number
				for(Entry<String, List<String>> elem : map.entrySet()){
					String[] tokens = elem.getKey().split("--");
					block = Integer.parseInt(tokens[tokens.length-1]);
					if(block != blockNumber)	continue;
					fileBlockName = elem.getKey();
					// get candidateNodes
					candidateNodes = elem.getValue();
				}
				
				ipFiles.add(fileBlockName);
			
				
				// Use scheduler to get best map node to dispatch to, maybe using locality information
				nodeId = JTDispatcher.scheduler.getBestMapLocation(candidateNodes);
			
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
				nodeId = JTDispatcher.scheduler.getBestReduceLocation();
				
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
			
			nextTask.setCurrNodeId(nodeId);
			System.out.println("Dispather trying to dispatch to " + 
							nodeId + " at port " + JTDispatcher.dispatchPort);
			Socket dispatchSocket = new Socket(nodeId, JTDispatcher.dispatchPort);
			message.setIpFiles(ipFiles);
			message.setOpFile(opFile);
			message.setMsgType("start");
			message.setJob(job.getJob());
			message.setTaskType(nextTaskType);
			message.setTaskId(nextTaskId);
			
			ObjectOutputStream dispatchStream = new ObjectOutputStream(dispatchSocket.getOutputStream());
			dispatchStream.writeObject(message);
			dispatchStream.close();
			dispatchSocket.close();
			
			return true;
		} catch (UnknownHostException e) {
			System.out.println("Dispatcher couldn't get localhost address for target node.");
			return false;
		} catch (IOException e) {
			System.out.println("Dispatcher couldn't get connection to target node.");
			e.printStackTrace();
			return false;
		} catch (NotBoundException e) {
			System.out.println("Dispatcher requested a service that was not bound to registry.");
			return false;
		}
	
	}
	
	// Pretty printing of the state of cluster, for DEBUG purposes
	
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
