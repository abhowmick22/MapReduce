package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;


import mapred.interfaces.Combiner;
import mapred.interfaces.Mapper;
import mapred.interfaces.Reducer;
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
				partition(output, buffer, numReducers);
				
				// sort the keys in each of the R lists
				ListIterator<ArrayList<Pair<String>>> it = buffer.listIterator();
				while(it.hasNext()){
					ArrayList<Pair<String>> list = it.next();
					sort(list);
					it.set(list);
				}
				
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
			
			try {
				// get the reducer from the parent job
				Reducer reducer = this.parentJob.getReducer();
				
				// TODO: shuffle using List of remote ipFiles
				String ipFile1 = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-1";
				String ipFile2 = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-2";
				
				// pull and aggregate those files into local file (F) on disk
				// concatenation of 1 & 2, just to test reducer
				String ipFile = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-3";
				
				// read records and sort keys in local input file
				BufferedReader file = new BufferedReader(new FileReader(ipFile));
				String record = null;
				
				// Initialise an output set
				List<Pair<String>> input = new ArrayList<Pair<String>>();
				
				// TODO: loop till RecordReader returns null
				Pair<String> p = null;
				while((record = file.readLine()) != null){
					// build the input
					String[] tokens = record.split("\\s*,\\s*");
					p = new Pair<String>();
					p.setFirst(tokens[0]);
					p.setSecond(tokens[1]);
					input.add(p);
				}
				
				// initialise an output table of K - <V1, V2>
				// this is the intermediate table which stores the list of all values per key
				Hashtable<String, ArrayList<String>> interTable = new  Hashtable<String, ArrayList<String> >();
				
				// loop till F returns null
				// for every KV pair
				ListIterator<Pair<String>> it = input.listIterator();
				while(it.hasNext()){
					Pair<String> pair = it.next();
					if(interTable.get(pair.getFirst()) == null)
						interTable.put(pair.getFirst(), new ArrayList<String>());
					interTable.get(pair.getFirst()).add(pair.getSecond());
				}
		
				// call the reduce method , for each list in the table
				String reduct = null;
				List<Pair<String>> output = new ArrayList<Pair<String>>();
				Pair<String> op = null;
				for(Entry<String, ArrayList<String>> elem : interTable.entrySet()){
					ArrayList<String> list = elem.getValue();
					reduct = reducer.reduce(list);
					op = new Pair<String>();
					op.setFirst(elem.getKey());
					//System.out.println(reduct);
					op.setSecond(reduct);
					output.add(op);
				}
				
				// sort the entries of table by key
				sort(output);
				//System.out.println(output.size());
				
				// Write table to op file on local disk
				String opFile = null;
				opFile = ipFile + "-4";
				PrintWriter writer = new PrintWriter(opFile, "UTF-8");
					for(int i=0; i<output.size(); i++){
					writer.println(output.get(i).getFirst().toString() + "," + 
										output.get(i).getSecond().toString());					
					}
				writer.close();
				
				// notify namenode to add this file to dfs
				
				// Indicate that it is finished to JobTracker (JTMonitor)
				} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
		}
	
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void partition(List<Pair<String>> output, 
								List<ArrayList<Pair<String>>> buffer, int numReducers){
		ListIterator<Pair<String>> oiterator = output.listIterator();
		int region;
		while(oiterator.hasNext()){
			Pair<String> p = oiterator.next();
			region = (Math.abs(p.getFirst().hashCode()))%numReducers;	// [0,R-1]
			buffer.get(region).add(p);
		}
		
	}
	
	// sort the keys within each partition before feeding into reducer (to be called by reducer)
	public void sort(List<Pair<String>> list){
		// Should do lexicographic sorting here
		Collections.sort(list, new Comparator<Pair<String>> () {
		    @Override
		    public int compare(Pair<String> m1, Pair<String> m2) {
		        return m1.getFirst().compareTo(m2.getFirst()); //descending
		    }
		});
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
