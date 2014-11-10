package mapred;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.ClientAPIMsg;
import mapred.messages.MasterToSlaveMsg;

/*
 * This is the object that processes each request that 
 * the JobTracker gets from the clientAPI
 */

public class JTProcessRequest implements Runnable {
	
	// request message to be processed
	private ClientAPIMsg request;
	// the jobtracker's list of mapreduce jobs
	ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	
	public JTProcessRequest(ClientAPIMsg request, ConcurrentHashMap<Integer, JobTableEntry> mapredJobs){
		this.request = request;
		this.mapredJobs = mapredJobs;
	}

	@Override
	public void run() {
		
		// extract the message type
		String reqType = request.getCommand();
		
		// Take actions based on request
		switch(reqType){
			case "launchJob":
					// TODO: find unique job id for this job
					int jobId = 1;
					
					MapReduceJob job = request.getJob();
					job.setJobId(jobId);
					String status = "Waiting";				
					JobTableEntry entry = new JobTableEntry(job, status);
					this.mapredJobs.put(jobId, entry);
					break;
					
			case "stopJob":
					
					try {
						int stopJobId = request.getJobId();
						stopSlaves(stopJobId);
						mapredJobs.remove(stopJobId);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}			
					// remove the job entry from mapredJobs table
					
					break;
					
			case "status":
					ClientAPIMsg reply = new ClientAPIMsg();
					reply.setCommand("reply");
					HashMap<Integer, String> report = getReport();
					reply.setReport(report);
					// send back reply to client
					try {
						String sourceAddr = request.getSourceAddr();
						Socket clientSocket = new Socket(sourceAddr, 20000);
						ObjectOutputStream replyStream = new ObjectOutputStream(clientSocket.getOutputStream());
						replyStream.writeObject(reply);
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
					
			default:
					System.out.println("Couldn't decipher client request\n");
					break;
		}
		
	}
	
	// helper function to send stop messages to all slaves running tasks of this job
	private void stopSlaves(int jobId) throws IOException{
		
		JobTableEntry job = this.mapredJobs.get(jobId);
		int jobStopId = job.getJob().getJobId();
		ConcurrentHashMap<Integer, TaskTableEntry> mapTasks = job.getMapTasks(); 
		ConcurrentHashMap<Integer, TaskTableEntry> reduceTasks = job.getReduceTasks();
		
		// send destroy commands to all map tasks
		sendStopMessages(mapTasks, jobStopId);
		// send destroy commands to all reduce tasks
		sendStopMessages(reduceTasks, jobStopId);
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
	
	// helper function to send destroy messages to all (map/reduce) tasks
	private void sendStopMessages(ConcurrentHashMap<Integer, TaskTableEntry> tasks, int jobStopId){
	
		Socket slaveSocket = null;
		
		for(TaskTableEntry task : tasks.values()){
			try {
				String nodeAddr = task.getCurrNodeId();
				MasterToSlaveMsg message = new MasterToSlaveMsg();
				message.setType("stop");
				message.setJobStopId(jobStopId);
				slaveSocket = new Socket(nodeAddr, 10001);
				ObjectOutputStream slaveStream = new ObjectOutputStream(slaveSocket.getOutputStream());
				slaveStream.writeObject(message);
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
