package mapred;

import java.util.concurrent.ConcurrentHashMap;

import mapred.messages.ClientAPIMsg;

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
					ConcurrentHashMap<Integer, TaskTableEntry> mapTasks = new 
								ConcurrentHashMap<Integer, TaskTableEntry>();
					// TODO: Populate mapTasks using ip file info
					
					JobTableEntry entry = new JobTableEntry(job, status, mapTasks);
					this.mapredJobs.put(jobId, entry);
				
					break;
			case "stopJob":
				
					break;
			case "status":
				
					break;
			default:
					System.out.println("Couldn't decipher client request\n");
					break;
		}
		
	}

}
