package mapred.interfaces;

import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;



/*
 * We allow our cluster to have different types of
 * schedulers, which have to implement this interface
 * 
 * TODO: Add methods to get node location for map and reduce tasks
 */

public interface Scheduler {
	
	/*
	* All required info such as the list of jobs, the locality info about data proximity etc
	* are supplied through setter methods
	*/
	// returns the <jobId, taskId> to be scheduled on datanode nodeId
	public Pair<JobTableEntry, TaskTableEntry> schedule();

}
