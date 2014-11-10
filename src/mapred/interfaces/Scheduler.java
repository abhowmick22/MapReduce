package mapred.interfaces;

import java.util.concurrent.ConcurrentHashMap;

import mapred.JobTableEntry;

/*
 * We allow our cluster to have different types of
 * schedulers, which have to implement this interface
 */

public interface Scheduler {
	
	// Takes in the list of jobs, the locality info about data proximity,
	// and returns the <jobId, taskId> to be scheduled on datanode nodeId
	public void schedule(ConcurrentHashMap<String, JobTableEntry> mapredJobs, Class<?> localityInfo,
								int jobId, int taskId, String nodeId);

}
