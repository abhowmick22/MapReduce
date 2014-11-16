package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;


import mapred.interfaces.Combiner;
import mapred.interfaces.Mapper;
import mapred.types.MapReduceJob;
import mapred.types.Pair;

/*
 * Objects of this instance are units of computation
 * this is run in a thread by the TaskTracker
 * 
 * Right now, we assume that a single task works on an entire file
 * chunk, there is no further splitting among partitions for tasks
 */

public class Task implements Runnable{
	
	// flag indicating whether this task should continue executing
	private volatile boolean alive;
	// The file block which this task takes as input
	private List<String> ipFileNames;
	// The job of which this task is part
	private MapReduceJob parentJob;
	// Type of task
	private String taskType;
	// Task Id
	private int taskId;
	// record numbers to read in case of map task
	private int readRecordStart;
	private int readRecordEnd;
	
	// Special constructor to create a map Task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId,
						int readRecordStart, int readRecordEnd){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "map"
		this.taskId = taskId;
		this.readRecordStart = readRecordStart;
		this.readRecordEnd = readRecordEnd;
	}
	
	// Special constructor to create a reduce task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "reduce"
		this.taskId = taskId;
	}
	
	
	@Override
	public void run() {
		
		this.alive = true;
		
		// If this is a map task
		if(this.taskType.equals("map")){

			try {
				// Get the mapper from the parent job
				Mapper mapper = this.parentJob.getMapper();
				
				// TODO: Initialise a RecordReader supplying (ipFile[0], readRecordStart, readRecordEnd)
				String ipFile = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input";
				BufferedReader input = new BufferedReader(new FileReader(ipFile));
				String record = null;
				
				// Initialise an output set
				List<Pair<String>> output = new ArrayList<Pair<String>>();
				
				// Determine the number of reducers (R) from the parent mapreduce job
				int numReducers = this.parentJob.getNumReducers();
				
				// initialise lists to which o/p kV pairs will be written
				List<ArrayList<Pair<String>>> buffer = new ArrayList<ArrayList<Pair<String>>>();
				for(int i=0; i< numReducers; i++){
					ArrayList<Pair<String>> newList = new ArrayList<Pair<String>>();
					buffer.add(i, newList);
				}
			
				// TODO: loop till RecordReader returns null
				while((record = input.readLine()) != null){
					// call the map method of mapper, supplying record and collecting output in OutputSet
					mapper.map(record, output);
				}
				
				// partition output to store them into the R lists
				ListIterator<Pair<String>> oiterator = output.listIterator();
				int region;
				while(oiterator.hasNext()){
					Pair<String> p = oiterator.next();
					region = p.getFirst().hashCode()%numReducers;	// [0,R-1]
					buffer.get(region).add(p);
				}
				
				// sort the keys in each of the R lists
				sort(buffer);
				
				// Run (optional) combiner stage on each of these R lists
				ArrayList<Pair<String>> op = null;
				if(this.parentJob.getIfCombiner()){
					System.out.println("true");
					Combiner combiner = this.parentJob.getCombiner();
					ListIterator<ArrayList<Pair<String>>> biterator = buffer.listIterator();
					while(biterator.hasNext()){
						ArrayList<Pair<String>> b = biterator.next();
						combiner.combine(b, op);
						biterator.set(op);
					}
				}
				
				// Flush them onto disk, each such file contains all KV pairs per partition (one per line)
				String opFile = null;
				ListIterator<ArrayList<Pair<String>>> biterator = buffer.listIterator();
				for(int i=0; i<numReducers; i++){
					ArrayList<Pair<String>> content = buffer.get(i);
					opFile = ipFile + "-" + (i+1);
					PrintWriter writer = new PrintWriter(opFile, "UTF-8");
					// write each pair into the file
					ListIterator<Pair<String>> line = content.listIterator();
					while(line.hasNext()){
						Pair<String> p = line.next();
						writer.println(p.getFirst().toString() + "," + p.getSecond().toString());
					}
					writer.close();
				}
				
				
				// Notify the name node, and ask to add this
				
				// Indicate that it is finished to JobTracker (JTMonitor)
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		// Else If this is a reduce task
		else{
			// get the reducer from the parent job
		
			// already have list of ipFileNames (remote)
		
			// pull and aggregrate those files into local file (F) on disk
		
			// initialise an output table of K - <V1, V2>
		
			// loop till F returns null
				// for every KV pair
		
				// call the reduce method , getting an op key and op value
		
				// add this op KV pair to the output table, appending to the value in table
		
			// sort the entries of output table by key, use sort()
		
			// Write table to op file on local disk
		
			// notify namenode to add this file to dfs
		
			// Indicate that it is finished to JobTracker (JTMonitor)
		
		}
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void partition(){
		
	}
	
	// sort the keys within each partition before feeding into reducer (to be called by reducer)
	public void sort(List<ArrayList<Pair<String>>> buffer){
		// Should do lexicographic sorting here
		;
	}
	
	// get the data from mapper to reducer nodes
	public void shuffle(){
		
	}
	
	// method to kill the thread running this Runnable
	// this will be called by tasktracker, which resets the alive flag
	// The flag is conitnually checked by the run method, which exits if it is set to false
	// ?? Returns true if the thread was successfully killed
	public void killThread(){
		this.alive = false;
		//while(!this.alive) ;	// cause the taskTracker to be spinning till the thread dies
		//return true;
	}

}
