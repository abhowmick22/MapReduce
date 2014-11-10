package mapred;

import java.util.concurrent.ConcurrentHashMap;

import mapred.interfaces.Scheduler;

/*
 * This object makes scheduling decisions - i.e it decides which jobs to be scheduled next 
 * It has very simple logic
 */

public class SimpleScheduler implements Scheduler{

	@Override
	public void schedule(ConcurrentHashMap<String, JobTableEntry> mapredJobs,
			Class<?> localityInfo, int jobId, int taskId, String nodeId) {
		// TODO Auto-generated method stub
		
	}

}
