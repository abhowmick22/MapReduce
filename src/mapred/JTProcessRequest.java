package mapred;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


import mapred.messages.ClientAPIMsg;
import mapred.messages.MasterToSlaveMsg;
import mapred.types.JobTableEntry;
import mapred.types.MapReduceJob;
import mapred.types.TaskTableEntry;

/*
 * This is the object that processes each request that 
 * the JobTracker gets from the clientAPI
 */

public class JTProcessRequest implements Runnable {
	
	// request message to be processed
	private ClientAPIMsg request;
	// the jobtracker's list of mapreduce jobs
	private ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// next jobId to be allotted
	private int nextJobId;
	// the type of commands expected
	private enum reqType {LAUNCH, STOP, STATUS};
	
	public JTProcessRequest(ClientAPIMsg request, ConcurrentHashMap<Integer, JobTableEntry> mapredJobs,
								int lastJobId){
		this.request = request;
		this.mapredJobs = mapredJobs;
		this.nextJobId = lastJobId; 
	}

	@Override
	public void run() {
		
		// extract the message type
		String reqType = request.getCommand();
		
		// Take actions based on request
			if( reqType.equals("launchJob")){
					// TODO: find unique job id for this job
					// For now, it's just a linear count, assuming not more than 100 jobs can co-exist
					this.nextJobId++;
				
					MapReduceJob job = request.getJob();
					job.setJobId(this.nextJobId);
					String status = "waiting";				
					JobTableEntry entry = new JobTableEntry(job, status);
					this.mapredJobs.put(this.nextJobId, entry);
					
			}
			else if(reqType.equals("stopJob")){
					try {
						// send stop commands to all slaves
						int stopJobId = request.getJobId();
						stopSlaves(stopJobId);
						// remove the job entry from mapredJobs table
						this.mapredJobs.remove(stopJobId);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}		
			}
					
			else{				// "status"
					ClientAPIMsg reply = new ClientAPIMsg();
					reply.setCommand("reply");
					HashMap<Integer, String> report = getReport();
					reply.setReport(report);
					// send back reply to client
					try {
						String sourceAddr = request.getSourceAddr();
						Socket clientSocket = new Socket(sourceAddr, 20001);
						ObjectOutputStream replyStream = new ObjectOutputStream(clientSocket.getOutputStream());
						replyStream.writeObject(reply);
						//printState();
						replyStream.close();
						clientSocket.close();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
			}
		
	}
	
	// helper function to send stop messages to all slaves running tasks of this job
	private void stopSlaves(int jobId) throws IOException{
		
		JobTableEntry job = this.mapredJobs.get(jobId);
		int jobStopId = job.getJob().getJobId();
		ConcurrentHashMap<Integer, TaskTableEntry> mapTasks = job.getMapTasks(); 
		ConcurrentHashMap<Integer, TaskTableEntry> reduceTasks = job.getReduceTasks();
		
		// send destroy commands to all map tasks
		// TODO: Uncomment the next statement
		//sendStopMessages(mapTasks, jobStopId);
		// send destroy commands to all reduce tasks
		// TODO: Uncomment the next statement
		//sendStopMessages(reduceTasks, jobStopId);
	}
	
	// helper function to send destroy messages to all (map/reduce) tasks
	private void sendStopMessages(ConcurrentHashMap<Integer, TaskTableEntry> tasks, int jobStopId){
	
		Socket slaveSocket = null;
		
		// check if tasks is not empty
		if(!tasks.isEmpty()){
			for(TaskTableEntry task : tasks.values()){
				try {
					String nodeAddr = task.getCurrNodeId();
					MasterToSlaveMsg message = new MasterToSlaveMsg();
					message.setMsgType("stop");
					message.setJobStopId(jobStopId);
					slaveSocket = new Socket(nodeAddr, 10001);
					ObjectOutputStream slaveStream = new ObjectOutputStream(slaveSocket.getOutputStream());
					slaveStream.writeObject(message);
					slaveStream.close();
					slaveSocket.close();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	// helper function to get report for this jobtracker
	private HashMap<Integer, String> getReport(){
		HashMap<Integer, String> report = new HashMap<Integer, String>();
		for(Map.Entry<Integer, JobTableEntry> entry : this.mapredJobs.entrySet()){
			 int jobId = entry.getKey();
			 String jobStatus = entry.getValue().getStatus();
			 report.put(jobId, jobStatus);
		}
		return report;
	}
		
	
	// Pretty printing of the state of cluster
	/*private void printState(){
		// print the jobs table
		for(JobTableEntry job : this.mapredJobs.values()){
			System.out.println("Job Id: " + job.getJob().getJobId() + " | Job Name: "  + job.getJob().getJobName() +
								" | Status: " + job.getStatus() +
								" | IP File Name: " + job.getJob().getIpFileName());
			System.out.println("\t\t---------Map Tasks--------------");
				for(TaskTableEntry mapTask : job.getMapTasks().values()){
					System.out.println("\t\t\tTask Id: " + mapTask.getTaskId() + " | Status: " + 
							mapTask.getStatus() + " | Node: " + mapTask.getCurrNodeId() +
							" | Start record: " + mapTask.getRecordRange().get(0) + 
							" | End record: " + mapTask.getRecordRange().get(1));
				}
				
		}
	}*/

}
