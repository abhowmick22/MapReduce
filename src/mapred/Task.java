package mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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
	// the record size
	private int recordSize;
	// The IP address of the TTMonitor
	private String taskmonitorIpAddr;
	// port on which to send message to TTMonitor
	private int taskmonitorPort;
	// local base directory
	private String localBaseDir;

	
	// reduce specific
	// The IP Addr of the namenode
	private String nameNode;					
	// The port of the namenode
	private int nameNodePort;
	// the port for datanode
	private int dataNodePort;
	
	// Special constructor to create a map Task
	public Task(List<String> ipFileNames, MapReduceJob job, int taskId,
						int readRecordStart, int readRecordEnd, String taskmonitorIpAddr,
						int taskmonitorPort, int recordSize, String localBaseDir){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = "map";
		this.taskId = taskId;
		this.readRecordStart = readRecordStart;
		this.readRecordEnd = readRecordEnd;
		this.taskmonitorIpAddr = taskmonitorIpAddr;
		this.taskmonitorPort = taskmonitorPort;
		this.recordSize = recordSize;
		this.localBaseDir = localBaseDir;
	}
	
	// Special constructor to create a reduce task
	public Task(List<String> ipFileNames, MapReduceJob job, int taskId, 
								String taskmonitorIpAddr, int taskmonitorPort, int recordSize,
								String nameNode, int nameNodePort, int dataNodePort, String localBaseDir){
		this.ipFileNames = ipFileNames;
		this.parentJob = job;
		this.taskType = "reduce";
		this.taskId = taskId;
		this.taskmonitorIpAddr = taskmonitorIpAddr;
		this.taskmonitorPort = taskmonitorPort;
		this.recordSize = recordSize;
		this.nameNode = nameNode;
		this.nameNodePort = nameNodePort;
		this.dataNodePort = dataNodePort;
		this.localBaseDir = localBaseDir;

	}
	
	
	@Override
	public void run() {
		this.alive = true;
		
		// If this is a map task
		if(this.taskType.equals("map")){
			try {
				// Get the mapper from the parent job
				Mapper mapper = this.parentJob.getMapper();
				
				// get a filename to read from
			    String ipFileName = this.ipFileNames.get(0);
				File ipFile = getLocalFile(ipFileName);
				RandomAccessFile file = new RandomAccessFile(ipFile.getAbsoluteFile(), "r");
				
				//BufferedReader input = new BufferedReader(new FileReader(file));
				String record = null;
				
				// Initialize an output set
				List<Pair<String, String>> output = new ArrayList<Pair<String, String>>();
				
				// Determine the number of reducers (R) from the parent mapreduce job
				int numReducers = this.parentJob.getNumReducers();
				
				// initialize lists to which o/p kV pairs will be written
				List<ArrayList<Pair<String, String>>> buffer = new ArrayList<ArrayList<Pair<String, String>>>();
				for(int i=0; i< numReducers; i++){
					ArrayList<Pair<String, String> > newList = new ArrayList<Pair<String, String> >();
					buffer.add(i, newList);
				}
				
				int totBytesRead = 0;
				int totalBytes = (this.readRecordEnd - this.readRecordStart + 1)*this.recordSize;
				// seek to proper offset
				file.seek(this.readRecordStart*this.recordSize);
				byte[] readBuffer = new byte[this.recordSize];
				int bytesRead = 1;
				while(totBytesRead < totalBytes && bytesRead > 0){
					bytesRead = file.read(readBuffer);
					// Do the actual map here
					record = new String(readBuffer);
					mapper.map(record, output);
					if(bytesRead > 0)	totBytesRead += bytesRead;
				}
				file.close();
				
				// partition output to store them into the R lists
				partition(output, buffer, numReducers);
				
				// sort the keys in each of the R lists
				ListIterator<ArrayList<Pair<String, String>>> it = buffer.listIterator();
				while(it.hasNext()){
					ArrayList<Pair<String, String>> list = it.next();
					sort(list);
					it.set(list);
				}
				
				// Run (optional) combiner stage on each of these R lists
				ArrayList<Pair<String, String>> op = null;
				if(this.parentJob.getIfCombiner()){
					System.out.println("combiner enabled");
					Combiner combiner = this.parentJob.getCombiner();
					ListIterator<ArrayList<Pair<String, String>>> biterator = buffer.listIterator();
					while(biterator.hasNext()){
						ArrayList<Pair<String, String>> b = biterator.next();
						combiner.combine(b, op);
						biterator.set(op);
					}
				}
				
				
				// Flush them onto disk, each such file contains all KV pairs per partition (one per line)
				ConcurrentHashMap<Integer, String> opFiles = new ConcurrentHashMap<Integer, String>();
				String fileName = null;
				String node = InetAddress.getLocalHost().getHostAddress();
				int partition = 0;
				for(int i=0; i<numReducers; i++){
					ArrayList<Pair<String, String>> content = buffer.get(i);
					partition = i;
					fileName = node + ":" + ipFileName + "-" + this.taskId + "-" + partition;	
				    RandomAccessFile intermediateFile = new 
				    		RandomAccessFile(getLocalFile(fileName).getAbsoluteFile(), "rw");
					// write each pair into the file
					ListIterator<Pair<String, String>> line = content.listIterator();
					while(line.hasNext()){
						Pair<String, String> p = line.next();
						intermediateFile.writeBytes(p.getFirst().toString() + "," + p.getSecond().toString() + "\n");
					}
					intermediateFile.close();
					opFiles.put(partition, fileName);
				}
				
				// TODO: Notify the name node, and ask to add this
				// Use RMI on namenode for this
				
				// Indicate that it is finished to TTMonitor
				sendFinishMessage("map", opFiles);
				
			} catch (FileNotFoundException e) {
				System.out.println("Mapper can't find input file.");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Mapper either can't read or write.");
				e.printStackTrace();
			}
		
		}
		
		// Else If this is a reduce task
		
		else{
			
			try {
				// get the reducer from the parent job
				Reducer reducer = this.parentJob.getReducer();
							
				// shuffle using List of remote and local ipFiles
				String ipFileName = shuffle(this.ipFileNames, this.taskId);
				File file = getLocalFile(ipFileName);
				
				// read records and sort keys in local input file
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String record = null;
				
				// Initialize an output set
				List<Pair<String, String>> input = new ArrayList<Pair<String, String>>();
				
				// loop till the end of ipfile
				Pair<String, String> p = null;
				while((record = reader.readLine()) != null){
					// build the input, read the tokenizer from the job parameters
					String[] tokens = record.split("\\s*,\\s*");
					p = new Pair<String, String>();
					if(tokens.length == 2){
						p.setFirst(tokens[0]);
						p.setSecond(tokens[1]);
						input.add(p);
					}
				}
				
				// initialize an output table of K - <V1, V2>
				// this is the intermediate table which stores the list of all values per key
				Hashtable<String, ArrayList<String>> interTable = new  Hashtable<String, ArrayList<String> >();
				
				// loop for every KV pair till F returns null
				ListIterator<Pair<String, String>> it = input.listIterator();
				while(it.hasNext()){
					Pair<String, String> pair = it.next();
					if(interTable.get(pair.getFirst()) == null)
						interTable.put(pair.getFirst(), new ArrayList<String>());
					interTable.get(pair.getFirst()).add(pair.getSecond());
				}
		
				// call the reduce method , for each list in the table
				String reduct = null;
				List<Pair<String, String>> output = new ArrayList<Pair<String, String>>();
				Pair<String, String> op = null;
				for(Entry<String, ArrayList<String>> elem : interTable.entrySet()){
					ArrayList<String> list = elem.getValue();
					reduct = reducer.reduce(list);
					op = new Pair<String, String>();
					op.setFirst(elem.getKey());
					op.setSecond(reduct);
					output.add(op);
				}
				
				// sort the entries of table by key
				sort(output);

				// TODO: Write to use specified output file
				String opFileName = ipFileName + "-out";
				File opFile = getLocalFile(opFileName);
				
				Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(opFile)));
					for(int i=0; i<output.size(); i++){
					writer.write(output.get(i).getFirst().toString() + "," + 
										output.get(i).getSecond().toString() + "\n");	
					writer.flush();
					}
				writer.close();
				
				// TODO: notify nameNode to add this file to dfs
				// ?? How to add this file to requested output location on dfs
				
				// Indicate that it is finished to TTMonitor
				sendFinishMessage("reduce", null);
				} catch (FileNotFoundException e) {
				System.out.println("Reducer can't find input file.");
				} catch (IOException e) {
				System.out.println("Reducer either can't read or write.");
				}
		}
	
	}
	
	// partition the keys into regions in order to be sent to appropriate reducers
	public void partition(List<Pair<String, String>> output, 
								List<ArrayList<Pair<String, String>>> buffer, int numReducers){
		ListIterator<Pair<String, String>> oiterator = output.listIterator();
		int region;
		while(oiterator.hasNext()){
			Pair<String, String> p = oiterator.next();
			region = (Math.abs(p.getFirst().hashCode()))%numReducers;	// [0,R-1]
			buffer.get(region).add(p);
		}
		
	}
	
	// sort the keys within each partition before feeding into reducer (to be called by reducer)
	public void sort(List<Pair<String, String>> list){
		// Doing lexicographic sorting here
		Collections.sort(list, new Comparator<Pair<String, String> > () {
		    @Override
		    public int compare(Pair<String, String> m1, Pair<String, String> m2) {
		        return m1.getFirst().compareTo(m2.getFirst()); //ascending
		    }
		});
	}
	
	// get the data from mapper to reducer nodes
	public String shuffle(List<String> ipFileNames, int reducerNum){
		
		// reducer ipFile on local filesystem
		String ipFileName = "reducer_input" + reducerNum;		// hard-coded value
		
		File ipFile = getLocalFile(ipFileName);
		Writer writer = null;
		try {
			ipFile.createNewFile();
			//writer = new PrintWriter(ipFile, "UTF-8");
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ipFile)));

		ListIterator<String> it = ipFileNames.listIterator();
		String fileLocation = null;
		String record = null;
		
		while(it.hasNext()){
			fileLocation = it.next();
			File readFile = getLocalFile(fileLocation);
			
			// TODO: Invoke RMI service on datanode using parts to get back file(?) object
			// For testing, assume file is on this node
			
				BufferedReader reader = new BufferedReader(new FileReader(readFile));
				while((record = reader.readLine())!=null){
					writer.write(record + "\n");
					writer.flush();
				}
				
			} 
		} catch (FileNotFoundException e) {
			System.out.println("Shuffle coudn't find input file.");
		} catch (IOException e) {
				System.out.println("Shuffle coudn't read input file.");
		}
		
		return ipFileName;
		
	}
	
	// TODO: method to kill the thread running this Runnable
	// this will be called by tasktracker, which resets the alive flag
	// The flag is continually checked by the run method, which exits if it is set to false
	// ?? Returns true if the thread was successfully killed
	public void killThread(){
		this.alive = false;
		//while(!this.alive) ;	// cause the taskTracker to be spinning till the thread dies
		//return true;
	}
	
	// TODO: get file handle on LFS by supplying dfs file name
	public File getLocalFile(String ipFile){
		// get System properties :
	    java.util.Properties properties = System.getProperties();
	    // to print all the keys in the properties map <for testing>
	    //properties.list(System.out);
	    // get Operating System home directory
	    String home = properties.get("user.home").toString();
	    //String home = this.localBaseDir;
	    // get Operating System separator
	    String separator = properties.get("file.separator").toString();
	    // your directory name
	    String directoryName = "15-640/project3/mapreduce/src/mapred/tests";
	    //String directoryName = "mapreduce/run";
	    // create your directory Object (wont harm if it is already there ... 
	    // just an additional object on the heap that will cost you some bytes
	    File dir = new File(home+separator+directoryName);
	    //  create a new directory, will do nothing if directory exists
	    dir.mkdir();    
	    File file = new File(dir,ipFile);
		return file;
	}
	
	public void sendFinishMessage(String taskType, ConcurrentHashMap<Integer, String> opFiles){
		Pair<Integer, Integer> finishedTask = new Pair<Integer, Integer>();
		finishedTask.setFirst(this.parentJob.getJobId());
		finishedTask.setSecond(this.taskId);
		SlaveToMasterMsg signal = new SlaveToMasterMsg();
		signal.setMsgType("finished");
		signal.setTaskType(taskType);
		
		if(taskType.equals("map"))	signal.setOpFiles(opFiles);
			
		signal.setFinishedTask(finishedTask);
		Socket monitorSocket;
		try {
			monitorSocket = new Socket(this.taskmonitorIpAddr, this.taskmonitorPort);
			ObjectOutputStream monitorStream = new ObjectOutputStream(monitorSocket.getOutputStream());
			monitorStream.writeObject(signal);
			monitorStream.close();
			monitorSocket.close();
		} catch (UnknownHostException e) {
			System.out.println("Can't identify Monitor to send finish message to.");
		} catch (IOException e) {
			System.out.println("Can't get connection Monitor to send finish message to.");
		}
		
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
