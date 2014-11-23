package mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import datanode.Node;
import dfs.DfsService;
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
	// the file to which output should be written by reducer
	private String opFileName;
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
	public Task(List<String> ipFileNames, String opFileName, MapReduceJob job, int taskId,
						int readRecordStart, int readRecordEnd, String taskmonitorIpAddr,
						int taskmonitorPort, int recordSize, String localBaseDir){
		this.ipFileNames = ipFileNames;
		this.opFileName = opFileName;
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
	public Task(List<String> ipFileNames, String opFileName, MapReduceJob job, int taskId, 
								String taskmonitorIpAddr, int taskmonitorPort, int recordSize,
								String nameNode, int nameNodePort, int dataNodePort, String localBaseDir){
		this.ipFileNames = ipFileNames;
		this.opFileName = opFileName;
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
				String mapperClassName = this.parentJob.getMapper();
				
				// get a filename to read from
			    String ipFileName = this.ipFileNames.get(0);
				File ipFile = getLocalFile(ipFileName);
				RandomAccessFile file = new RandomAccessFile(ipFile.getAbsoluteFile(), "r");
				//System.out.println("mapper got ipFile");
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
				
				System.out.println("mapper starting");
				
				if(this.parentJob.getInputSplit().getDelimiter().equals("b")) {
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
    
    					//mapper.map(record, output);
    					performTask("map", mapperClassName, "map", output, record, null);
    					
    					if(bytesRead > 0)	totBytesRead += bytesRead;
    				}
				} else if (this.parentJob.getInputSplit().getDelimiter().equals("c")) {
				    int taskID = this.getTaskId();
				    int totalSizeSoFar = 0;
				    for(int i=0; i<taskID; i++) {
				        //skip certain lines
				        while(true) {
    				        record = file.readLine();
    				        totalSizeSoFar += record.length();
    				        if(totalSizeSoFar > this.parentJob.getSplitSize()) {
    				            break;
    				        }
				        }
				        totalSizeSoFar = 0;
				    }
				    while(true) {
                        record = file.readLine();
                        performTask("map", mapperClassName, "map", output, record, null);
                        totalSizeSoFar += record.length();
                        if(record == null || totalSizeSoFar > this.parentJob.getSplitSize()) {
                            break;
                        }
                    }    				                   
				}
				file.close();
				
				System.out.println("mapper done");
				
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
				
				// prepend host address since this will be used to determine the datanode later through rmi
				String node = InetAddress.getLocalHost().getHostAddress();
				if(node.charAt(node.length()-1) == '\\')	node = node.substring(0, node.length()-1);

				int partition = 0;
				for(int i=0; i<numReducers; i++){
					ArrayList<Pair<String, String>> content = buffer.get(i);
					partition = i+1;
					fileName = node + ":" + ipFileName + "--" + this.taskId + "--" + partition;	
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
				
				// TODO: No need to notify the name node
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
				String reducerClassName = this.parentJob.getReducer();
				int reducerNum = this.taskId;			
				
				// shuffle using List of remote and local ipFiles
				String ipFileName = shuffle(this.ipFileNames, reducerNum);
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
					
					// Use reflections to do reduce here
					//reduct = reducer.reduce(list);
			        reduct = performTask("reduce", reducerClassName, "reduce", null, null, list);

					op = new Pair<String, String>();
					op.setFirst(elem.getKey());
					op.setSecond(reduct);
					output.add(op);
				}
				
				// sort the entries of table by key
				sort(output);

				// TODO: Check writing to user specified output file
				String opFileNm = this.parentJob.getJobName() + "-reducer-" + (reducerNum+1) + "-output.txt";
				File opFile = getLocalFile(opFileNm);
				
				Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(opFile)));
					for(int i=0; i<output.size(); i++){
					writer.write(output.get(i).getFirst().toString() + "," + 
										output.get(i).getSecond().toString() + "\n");	
					writer.flush();
					}
				writer.close();
				
				// TODO: Check notify nameNode to add this file to dfs
				try {
					String opFileDir = this.opFileName;		// this gives me output folder
					if(opFileDir.charAt(opFileDir.length()-1) != '/')	opFileDir += "/";
					Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNode, nameNodePort);
					DfsService nameNodeService = (DfsService) nameNodeRegistry.lookup("DfsService");
					//nameNodeService.updateActiveNodes(activeNodeList, InetAddress.getLocalHost().getHostAddress());
					if(opFileDir.charAt(opFileDir.length()-1) != '/')	opFileDir += "/";
					System.out.println(opFileDir + opFileNm);
					nameNodeService.addOutputFileToDfs(opFileDir + opFileNm, this.parentJob.getUserName(), 
															InetAddress.getLocalHost().getHostName());
				} catch (RemoteException e) {
					System.out.println("JTPolling: Got a remote method exception.");
					
				} catch (NotBoundException e) {
					System.out.println("JTPolling: Service requested not available in registry.");
				} catch (UnknownHostException e) {
					System.out.println("JTPolling: Could not get local host address.");
				}
				
				
				// Indicate that it is finished to TTMonitor
				sendFinishMessage("reduce", null);
				} catch (FileNotFoundException e) {
				System.out.println("Reducer can't find input file.");
				e.printStackTrace();
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
		String ipFileName = this.parentJob.getJobName() + "-reducer-" + (reducerNum+1) + "-input.txt";		// hard-coded value
		String fileLocation = null;
		//File ipFile = getLocalFile(ipFileName);
		//Writer writer = null;
		try {
			//ipFile.createNewFile();
			//writer = new PrintWriter(ipFile, "UTF-8");
			//writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ipFile)));

		ListIterator<String> it = ipFileNames.listIterator();
		
		String record = null;
		
        //RandomAccessFile readFile = new RandomAccessFile(getLocalFile(ipFileName).getAbsoluteFile(),"rw");			
    	BufferedWriter writer = new BufferedWriter(new FileWriter(getLocalFile(ipFileName).getAbsoluteFile()));

    	
		int read = 0;		
		while(it.hasNext()){
			fileLocation = it.next();
			
			// split file location into node and filePath 
			String[] tokens = fileLocation.split(":");
			String dataNodeIP = tokens[0];
			//fileLocation = tokens[1];
			
			//File readFile = getLocalFile(fileLocation);	// this has to be obtained from datanode
		
			
			// TODO: Check Invoke RMI service on datanode to get remote files
			try {
				Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNodeIP, dataNodePort);
				Node dataNode = (Node) dataNodeRegistry.lookup("DataNode");
				//dataNodeService.addOutputFileToDfs(opFileDir + "/" + opFileName, this.parentJob.getUserName(), 
														//InetAddress.getLocalHost().getHostName());
				System.out.println("fds");

				// get the remote file
				String remoteFilePath = this.localBaseDir + fileLocation;
				System.out.println("remote file path is " + remoteFilePath);
                byte[] bytes = new byte[1000];
                int start = 0;
                while((bytes = dataNode.getFile(remoteFilePath, start)) != null) {
                	int i=0;
                	for( i=0;i<bytes.length && bytes[i]!=0; i++){}
                	String appendand = new String(bytes, 0, i);
                	
                	writer.append(appendand);
                	System.out.println(appendand.length());
                	writer.flush();
                	bytes = new byte[1000];
                    start += 1000;
                }
                System.out.println("bytes read are " + start);
			} catch (RemoteException e) {
				System.out.println("JTPolling: Got a remote method exception.");
			} catch (NotBoundException e) {
				System.out.println("JTPolling: Service requested not available in registry.");
			} catch (UnknownHostException e) {
				System.out.println("JTPolling: Could not get local host address.");
			}

			
            //readFile.close();

            	/*
				BufferedReader reader = new BufferedReader(new FileReader(readFile));
				while((record = reader.readLine())!=null){
					writer.write(record + "\n");
					writer.flush();
				}
				*/
			}
		writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("Shuffle coudn't find input file.");
		} catch (IOException e) {
			e.printStackTrace();
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
	    //String directoryName = "15-640/project3/mapreduce/src/mapred/tests";
	    String directoryName = this.localBaseDir;
	    //String directoryName = "mapreduce/run";
	    // create your directory Object (wont harm if it is already there ... 
	    // just an additional object on the heap that will cost you some bytes
	    //File dir = new File(home+separator+directoryName);
	    //File dir = new File(directoryName);
	    File file = new File(directoryName + ipFile);
	    //  create a new directory, will do nothing if directory exists
	    //if(!dir.exists())	dir.mkdir();    
	    //File file = new File(dir,ipFile);
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
	
	// method to perform the map or reduce method
	// output and record set to null for reduce
	// list and reduct set to null for map
	public String performTask(String taskType, String className, String methodName, 
								List<Pair<String, String>> output, String record, ArrayList<String> list){
		String result = null;
		File jarFile = new File(this.parentJob.getJarPath());
		System.out.println(jarFile.getAbsolutePath());
        java.net.URL[] url = new java.net.URL[1];
        try {
            url[0] = jarFile.toURI().toURL();
        }
        catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        java.net.URLClassLoader urlClassLoader = new java.net.URLClassLoader(url, this.getClass().getClassLoader());
        Class<?> taskClass = null;
        
        try {
            taskClass = Class.forName(className, true, urlClassLoader);
            java.lang.reflect.Method method;
            if(taskType.equals("map")){
            	method = taskClass.getDeclaredMethod (methodName, String.class, List.class);
            	Object instance = taskClass.newInstance();
            	method.invoke(instance, record, output);
            }
            else{
            	method = taskClass.getDeclaredMethod (methodName, List.class);
            	Object instance = taskClass.newInstance();
            	result = (String) method.invoke(instance, list);
            	
            }
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		
		return result;
	}

}
