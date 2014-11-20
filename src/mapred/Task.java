package mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


import mapred.interfaces.Combiner;
import mapred.interfaces.Mapper;
import mapred.interfaces.Reducer;
import mapred.messages.SlaveToMasterMsg;
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
	// The IP address of the JobTracker
	private String taskmonitorIpAddr;
	
	// Special constructor to create a map Task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId,
						int readRecordStart, int readRecordEnd, String taskmonitorIpAddr){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "map"
		this.taskId = taskId;
		this.readRecordStart = readRecordStart;
		this.readRecordEnd = readRecordEnd;
		this.taskmonitorIpAddr = taskmonitorIpAddr;
	}
	
	// Special constructor to create a reduce task
	public Task(List<String> ipFileNames, MapReduceJob job, String taskType, int taskId, 
								String taskmonitorIpAddr){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = taskType;
		// Ensure that this taskType is "reduce"
		this.taskId = taskId;
		this.taskmonitorIpAddr = taskmonitorIpAddr;
	}
	
	
	@Override
	public void run() {
		
		this.alive = true;
		
		// Initialise parameters for file IO
		// get System properties :
	    java.util.Properties properties = System.getProperties();
	    // to print all the keys in the properties map <for testing>
	    //properties.list(System.out);
	    // get Operating System home directory
	    String home = properties.get("user.home").toString();
	    // get Operating System separator
	    String separator = properties.get("file.separator").toString();
	    // your directory name
	    String directoryName = "15-640/project3/mapreduce/src/mapred/tests";
	    // create your directory Object (wont harm if it is already there ... 
	    // just an additional object on the heap that will cost you some bytes
	    File dir = new File(home+separator+directoryName);
	    //  create a new directory, will do nothing if directory exists
	    dir.mkdir();    
		
		// If this is a map task
		if(this.taskType.equals("map")){

			try {
				// Get the mapper from the parent job
				Mapper mapper = this.parentJob.getMapper();
				
				// get a filename to read from
				//String ipFile = "/home/abhishek/15-640/project3/mapreduce/src/mapred/tests/test_input";
				//String ipFile = this.ipFileNames.get(0);
			    String ipFile = this.ipFileNames.get(0);
			    File file = new File(dir,ipFile);
				
				
				BufferedReader input = new BufferedReader(new FileReader(file));
				String record = null;
				
				// Initialize an output set
				List<Pair<String>> output = new ArrayList<Pair<String>>();
				
				// Determine the number of reducers (R) from the parent mapreduce job
				int numReducers = this.parentJob.getNumReducers();
				
				// initialize lists to which o/p kV pairs will be written
				List<ArrayList<Pair<String>>> buffer = new ArrayList<ArrayList<Pair<String>>>();
				for(int i=0; i< numReducers; i++){
					ArrayList<Pair<String>> newList = new ArrayList<Pair<String>>();
					buffer.add(i, newList);
				}
			
				// loop till RecordReader returns null
				int recordsRead = 0;
				while(recordsRead < this.readRecordStart){
					record = input.readLine();
					recordsRead++;
				}
				recordsRead--;
				while(recordsRead < this.readRecordEnd){
					// call the map method of mapper, supplying record and collecting output in OutputSet
					record = input.readLine();
					mapper.map(record, output);
					recordsRead++;
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
				ConcurrentHashMap<Integer, String> opFiles = new ConcurrentHashMap<Integer, String>();
				String fileName = null;
				ListIterator<ArrayList<Pair<String>>> biterator = buffer.listIterator();
				String node = InetAddress.getLocalHost().getHostAddress();
				int partition = 0;
				for(int i=0; i<numReducers; i++){
					ArrayList<Pair<String>> content = buffer.get(i);
					partition = i+1;
					fileName = node + ":" + ipFile + "-" + partition;			
				    File intermediateFile = new File(dir,fileName);

					intermediateFile.createNewFile();
					Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(intermediateFile)));
					// write each pair into the file
					ListIterator<Pair<String>> line = content.listIterator();
					while(line.hasNext()){
						Pair<String> p = line.next();
						writer.write(p.getFirst().toString() + "," + p.getSecond().toString() + "\n");
						writer.flush();
					}
					writer.close();
					opFiles.put(partition, fileName);
				}
				
				// Notify the name node, and ask to add this
				// Use RMI on namenode for this
				
				// Indicate that it is finished to TTMonitor
				Pair<Integer> finishedTask = new Pair<Integer>();
				finishedTask.setFirst(this.parentJob.getJobId());
				finishedTask.setSecond(this.taskId);
				SlaveToMasterMsg signal = new SlaveToMasterMsg();
				signal.setMsgType("finished");
				signal.setTaskType("map");
				signal.setOpFiles(opFiles);
				signal.setFinishedTask(finishedTask);
				Socket monitorSocket = new Socket(this.taskmonitorIpAddr, 10002);
				ObjectOutputStream monitorStream = new ObjectOutputStream(monitorSocket.getOutputStream());
				monitorStream.writeObject(signal);
				monitorStream.close();
				monitorSocket.close();
				
				System.out.println("Map task with Job id " + this.parentJob.getJobId() + " and task id " + this.taskId + " finished. ");
				
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
				
				//ipFile[0] = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-1";
				//ipFile[1] = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-2";
				
				// pull and aggregate those files into local file (F) on disk
				// use RMI on datanode for this
				//String ipFile = "/home/abhishek/15-640/project3/mapreduce/src/mapred/test_input-3";
				
				// shuffle using List of remote ipFiles
				// the input file names are already in ipFileNames
				String ipFile = shuffle(this.ipFileNames);

				// read records and sort keys in local input file
				BufferedReader file = new BufferedReader(new FileReader(ipFile));
				String record = null;
				
				// Initialise an output set
				List<Pair<String>> input = new ArrayList<Pair<String>>();
				
				// loop till the end of ipfile
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
					op.setSecond(reduct);
					output.add(op);
				}
				
				// sort the entries of table by key
				sort(output);
				//System.out.println(output.size());
				
				// Write table to op file on local disk
			    //String ipFile = this.;
			    //File file = new File(dir,ipFile);
				String opFile = null;
				opFile = ipFile + "-out";
				PrintWriter writer = new PrintWriter(opFile, "UTF-8");
					for(int i=0; i<output.size(); i++){
					writer.println(output.get(i).getFirst().toString() + "," + 
										output.get(i).getSecond().toString());					
					}
				writer.close();
				
				// notify namenode to add this file to dfs
				// ?? How to add this file to requested output location on dfs
				
				
				// Indicate that it is finished to TTMonitor
				Pair<Integer> finishedTask = new Pair<Integer>();
				finishedTask.setFirst(this.parentJob.getJobId());
				finishedTask.setSecond(this.taskId);
				SlaveToMasterMsg signal = new SlaveToMasterMsg();
				signal.setMsgType("finished");
				signal.setTaskType("reduce");
				Socket monitorSocket = new Socket(this.taskmonitorIpAddr, 10002);
				//Socket masterSocket = new Socket(InetAddress.getLocalHost().getHostName(), 10002);
				ObjectOutputStream monitorStream = new ObjectOutputStream(monitorSocket.getOutputStream());
				monitorStream.writeObject(signal);
				monitorStream.close();
				monitorSocket.close();
				
				System.out.println("Reduce task with Job id " + this.parentJob.getJobId() + " and task id " + this.taskId + " finished. ");

				
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
	public String shuffle(List<String> ipFileNames){
		
		// reducer ipFile on local filesystem
		String ipFile = "reducer_input";
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(ipFile, "UTF-8");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ListIterator<String> it = ipFileNames.listIterator();
		String fileLocation = null;
		String filePath = null;
		String sourceNode = null;
		String record = null;
		
		while(it.hasNext()){
			fileLocation = it.next();
			String[] parts = fileLocation.split(":");
			System.out.println(parts[0]);
			System.out.println(parts[1]);
			sourceNode = parts[0];
			filePath = parts[1];
			
			// TODO: Invoke RMI service on datanode using parts to get back file(?) object
			// For testing, assume file is on this node
			try {
				BufferedReader reader = new BufferedReader(new FileReader(filePath));
				while((record = reader.readLine())!=null){
					writer.println(record);
				}
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
			
		return ipFile;
	}
	
	// method to kill the thread running this Runnable
	// this will be called by tasktracker, which resets the alive flag
	// The flag is continually checked by the run method, which exits if it is set to false
	// ?? Returns true if the thread was successfully killed
	public void killThread(){
		this.alive = false;
		//while(!this.alive) ;	// cause the taskTracker to be spinning till the thread dies
		//return true;
	}
	
	// Return if the task is alive
	public boolean isAlive(){
		return this.alive;
	}
	
	// Get task id of this task
	public int getTaskId(){
		return this.taskId;
	}

}
