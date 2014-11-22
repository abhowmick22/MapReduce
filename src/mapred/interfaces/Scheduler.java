package mapred.interfaces;

import java.util.List;

import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;



/*
 * We allow our cluster to have different types of
 * schedulers, which have to implement this interface
 * 
 */

public interface Scheduler {
	
	/*
	* All required info such as the list of jobs, the locality info about data proximity etc
	* are supplied through setter methods
	*/
	// returns the <jobId, taskId> to be scheduled on datanode nodeId
	public Pair<JobTableEntry, TaskTableEntry> schedule();
	
	// return the IP Addr to which we need to send the mapper
	public String getBestMapLocation(List<String> candidateNodes);
	
	// return the IP Addr to which we need to send the reducer
	public String getBestReduceLocation();

}
