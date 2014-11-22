package mapred;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Scheduler;
import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

/*
 * This object makes scheduling decisions - i.e it decides which jobs to be scheduled next 
 * It has very simple logic
 * 
 */

public class SimpleScheduler implements Scheduler{
	
	// list of mapredJobs to be scheduled
	private ConcurrentHashMap<Integer,JobTableEntry> mapredJobs;
	// Last scheduled job, for Round Robin Scheduler
	private int lastScheduledJob;
	// Last scheduled task, for above job
	private int lastScheduledTask;
	// IP Addr for namenode
	private String nameNode;
	// the port of the namenode
	private int nameNodePort;
	// map of all nodes in the system with status and load
	private ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	
	// constructor with handle to mapredJobs
	public SimpleScheduler(ConcurrentHashMap<Integer,JobTableEntry> mapredJobs, 
								ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
								String nameNode, int nameNodePort){
		this.mapredJobs = mapredJobs;
		this.clusterNodes = clusterNodes;
		this.nameNode = nameNode;
		this.nameNodePort = nameNodePort;
	}
	

	@Override
	public Pair<JobTableEntry, TaskTableEntry> schedule() {
		
		// choose next job
		int numJobs = this.mapredJobs.size();

		Pair<JobTableEntry, TaskTableEntry> next = null;
		
		// check if no jobs are in mapredJobs
		// to be returned
		if(numJobs == 0)	return next;
		
		for(int i=0; i<numJobs; i++){
			String status = this.mapredJobs.get(i).getStatus();
			if(status.equals("done")){
				continue;
			}
			else{			// "waiting" or "map" or "reduce"
				// check if undispatched task exists
				String jobStatus = this.mapredJobs.get(i).getStatus();
				int nextTaskId = -1;				// not valid task
				String nextTaskType;
				ConcurrentHashMap<Integer, TaskTableEntry> tasks = null;
				
				if(!jobStatus.equals("reduce")){
					tasks = this.mapredJobs.get(i).getMapTasks();
					nextTaskType = "map";
				}
				else{
					tasks = this.mapredJobs.get(i).getReduceTasks();
					nextTaskType = "reduce";
				}
					
				int numTasks = tasks.size();
				boolean taskAvl = false;				// flag indicating if any tasks available
				for(int j=0; j<numTasks; j++){
					if(tasks.get(j).getStatus().equals("waiting")){
						nextTaskId = j;
						taskAvl = true;
					}
				}
				
				// If a dispatchable task is available
				if(taskAvl){
					// return this job
					//nextJobId = candidateJob;
					this.mapredJobs.get(i).setStatus(nextTaskType);
					//tasks.get(nextTaskId).setStatus("running");
					next = new Pair<JobTableEntry, TaskTableEntry>();
					next.setFirst(this.mapredJobs.get(i));
					next.setSecond(tasks.get(nextTaskId));
					break;
				}
			}
		}
		return next;
	
	}
	
	// TODO: Check this
	// return the IP Addr to which we need to send the mapper
	// given a list of potential nodes
	public String getBestMapLocation(List<String> candidateNodes){
		String chosenNode =  null;
		// The heavy lifting of locality information using namenode map has been done in dispatcher itself
		// Now just return the node that has minimum load amongst these
		int minLoad = 100;
		String currNode;
		int currLoad = 100;
		ListIterator<String> it = candidateNodes.listIterator();
		while(it.hasNext()){
			currNode = it.next();
			currLoad = this.clusterNodes.get(currNode).getSecond();
			if( currLoad < minLoad){
				minLoad = currLoad;
				chosenNode = currNode;
			}
		}
		
		//chosenNode = "ghc54.ghc.andrew.cmu.edu";		// for testing on cluster
		 //chosenNode = InetAddress.getLocalHost().getHostAddress();		// placeholder for testing
		return chosenNode;
	}
	
	// TODO: Check this
	// It chooses the node which has least load
	// return the IP Addr to which we need to send the reducer
	public String getBestReduceLocation(){
		String chosenNode =  null;
		//chosenNode = InetAddress.getLocalHost().getHostAddress();		// placeholder for testing
		 int size = this.clusterNodes.size();
		//System.out.println("s" + this.clusterNodes.size());

		 PriorityQueue<Entry<String, Pair<String, Integer>>> reducerLocations = 
				 new PriorityQueue<Entry<String, Pair<String, Integer>>>(size, 
				 							new Comparator<Entry<String, Pair<String, Integer>> > () {
		 @Override
		    public int compare(Entry<String, Pair<String, Integer>> m1, 
		    							Entry<String, Pair<String, Integer>> m2) {
		        return m1.getValue().getSecond().compareTo(m2.getValue().getSecond()); 
		    }
		 });
 
		 
		 for(Entry<String, Pair<String, Integer>> elem : this.clusterNodes.entrySet()){
			if(elem.getValue().getFirst().equals("up")) 
				reducerLocations.add(elem);
		 }
		 
		 Entry<String, Pair<String, Integer>> chosen = reducerLocations.poll();
		 //System.out.println("chosen node is " + result);
		 chosenNode = chosen.getKey();
		return chosenNode;
	}
	
	public void setLastScheduledJob(int lastScheduledJob){
		this.lastScheduledJob = lastScheduledJob;
	}
	
	public void setLastScheduledTask(int lastScheduledTask){
		this.lastScheduledTask = lastScheduledTask;
	}

	public int getLastScheduledJob(){
		return this.lastScheduledJob;
	}
	
	public int getLastScheduledTask(){
		return this.lastScheduledTask;
	}
}
