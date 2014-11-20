package mapred.interfaces;

import java.util.concurrent.ConcurrentHashMap;

import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;



/*
 * We allow our cluster to have different types of
 * schedulers, which have to implement this interface
 */

public interface Scheduler {
	
	/*
	* All required info such as the list of jobs, the locality info about data proximity etc
	* are supplied through setter methods
	*/
	// returns the <jobId, taskId> to be scheduled on datanode nodeId
	public Pair<JobTableEntry, TaskTableEntry> schedule();

}
