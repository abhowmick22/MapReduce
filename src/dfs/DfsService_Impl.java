package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import dfs.exceptions.DfsFileNotFound;
import dfs.exceptions.DuplicateFileException;
import dfs.exceptions.InvalidPathException;

public class DfsService_Impl implements DfsService {
	
final String _dfsPathIndentifier = "/dfs/";    //every path on dfs should start with this
    
	//TODO: make everything synchornized and concurrent
    private Map<String, Integer> _dataNodeLoad;     	//maintains a map between datanode and load on it in terms of number of blocks stored on it
    private int _repFactor;                         	//replication factor
    private String[] _dataNodeNames;                	//list of datanode names
    private int _dataNodesNum;                      	//total number of datanodes
    private DfsStruct _rootStruct;                  	//the root of the trie which represents the directory structure
    private int _nameNodePort;                      	//port that namenode listens to
    private String _localBaseDir;						//base directory on the local file system of each datanode
    private Map<String, DatanodeMetadata> _datanodeMap;	//map from datanode name to DatanodeMetadata that stores info about file blocks stored in that datanode
        
    private enum ConfigFileKeys{
    	TotalDataNodes,
    	DataNodeNames,
    	ReplicationFactor,
    	NameNodePort,
    	LocalBaseDir
    }
    
    /**
     * Initializes the DFS on the node from where it is run. It needs the configfile for initialization.
     */
    public void dfsInit() {
        FileReader fr = null;
        try {
            fr = new FileReader("src/dfs/tempDfsConfigFile");	//TODO: change the name
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line=br.readLine())!=null) {  
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                //check which key has been read, and initialize the appropriate global variable
                //fortunately JRE 7 has switch case for Strings
                ConfigFileKeys key = ConfigFileKeys.valueOf(keyValue[0].replaceAll("\\s", ""));
                switch(key) {
                    case TotalDataNodes: {
                        _dataNodesNum = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        break;
                    }
                    case DataNodeNames: {
                        _dataNodeNames = keyValue[1].split(",");
                        //remove whitespaces
                        for(int i=0; i<_dataNodeNames.length; i++) {
                            _dataNodeNames[i] = _dataNodeNames[i].replaceAll("\\s", "");
                        }
                        break;
                    }
                    case ReplicationFactor: {
                        _repFactor = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        break;
                    }
                    case NameNodePort: {
                        _nameNodePort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        break;
                    }
                    case LocalBaseDir: {
                    	_localBaseDir = keyValue[1].replaceAll("\\s", "");
                    	break;
                    }
                    default: {
                        System.out.println("Unrecognized key in config file: " + keyValue[0]);
                        break;
                    }
                }
            }
            
            //Initialize the trie for storing DFS and corresponding local paths
            //Step 1: create the root node
            _rootStruct = new DfsStruct("dfs", "/dfs/");
            _datanodeMap = new ConcurrentHashMap<String, DatanodeMetadata>();
            
        }
        catch (FileNotFoundException e) {
            System.out.println("EXCEPTION: Config file not found.");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Config file IO exception.");
            System.exit(0);
        }
        
        _dataNodeLoad = new ConcurrentHashMap<String, Integer>();
        for(String dataNode: _dataNodeNames) {
            System.out.println(dataNode);
            _dataNodeLoad.put(dataNode, 0);
        }        
        
    }
    
    /**
     * Checks the validity of the file path provided by the user.
     * @param path Path for a file provided by user.
     * @param username Username of the user 
     * @return Whether the path is valid (Boolean).
     */
    public synchronized boolean checkPathValidity(String path, String username) {
        if(!path.startsWith("/dfs/"+username+"/") || !path.endsWith(".txt")) {
        	//user cannot add directories without adding files
        	return false;
        } else {
        	//check if none of the directory names end with ".txt"
        	String[] dirFileNames = path.split("/");
        	for(int i=3; i<dirFileNames.length-1; i++) {
        		if(dirFileNames[i].endsWith(".txt")) {
        			return false;
        		}
        	}
        }
        return true;
    }
    
    /**
     * Checks if a particular file exists in the user's directory.
     * @param path The path to the file.
     * @param username The username of the user trying to access the file.
     * @return Whether the file exists in user's subdirectory on DFS.
     */
    public synchronized boolean checkFileExists(String path, String username, boolean skipPathValidityTest) throws RemoteException {
    	if(!skipPathValidityTest) {
    		if(!checkPathValidity(path, username)) {
	    		throw new InvalidPathException();
    		}
    	}
    	String[] dirFileNames = path.split("/");
    	DfsStruct tempStruct = _rootStruct.getSubDirsMap().get(username);
    	//dirFileNames[0] == "", so we start at index 1
    	for(int i=3; i<dirFileNames.length-1; i++) {
    		//check if the directory structure matches the path
    		if(tempStruct == null || !tempStruct.getSubDirsMap().containsKey(dirFileNames[i])) {
    			return false;
    		}
    		tempStruct = tempStruct.getSubDirsMap().get(dirFileNames[i]);
    	}
    	if(tempStruct != null && tempStruct.getFilesInDir().containsKey(dirFileNames[dirFileNames.length-1])) {
    		return true;
    	}
    	return false;
    }
    
    /**
     * Adds a file given by the user to the DFS.
     * @param path The DFS path of the file.
     * @param username The username of the user adding the file.
     * @param numBlocks Expected number of blocks that the file will be divided into. The expected number of blocks is used because
     * we do not perform division of files into blocks before knowing where these blocks will have to be sent.
     * @return A map of block numbers to the datanode names where the individual blocks should go, according to the replication factor.
     * @throws RemoteException
     */
    public synchronized Map<Integer, List<String>> addFileToDfs(String path, String username, int numBlocks) throws RemoteException {
    	if(!checkPathValidity(path, username)) {
    		//invalid path
    		throw new InvalidPathException(
    				"Invalid DFS path: \n" +
    				"1. Check if file name ends with \".txt\" \n" +
    				"2. No directory name ends with \".txt\" \n" +
    				"3. Username is correct in the path");
    	}  
    	if(checkFileExists(path, username, true)) {
    		//file already exists
    		throw new DuplicateFileException();
    	}
    	
    	String[] dirFileNames = path.split("/");
    	int pathLength = dirFileNames.length;
    	String addPath = "/dfs/";	//path to be added to each node
    	addPath = addPath + username + "/";
    	//create user directory if it doesn't already exist
    	if(!_rootStruct.getSubDirsMap().containsKey(username)) {    		
    		DfsStruct newDir = new DfsStruct(username, addPath);
    		_rootStruct.getSubDirsMap().put(username, newDir);
    	}
    	DfsStruct parent = _rootStruct.getSubDirsMap().get(username);
    	//now traverse the directory structure till the filename is expected to be encountered (i.e. pathLength-1)
    	for(int i=3; i<pathLength-1; i++) {
    		if(parent.getSubDirsMap().containsKey(dirFileNames[i])) {
    			//keep going till the end to create the directory structure    			
    			parent = parent.getSubDirsMap().get(dirFileNames[i]);
    		} else {
    			//directory name valid, but directory does not exist, so create directory
    			addPath = addPath + dirFileNames[i] + "/";
    			DfsStruct newStruct = new DfsStruct(dirFileNames[i], addPath);
    			parent.getSubDirsMap().put(dirFileNames[i], newStruct);
    			parent = newStruct;
    		}
    	}
    	
    	//add file to DFS
		DfsMetadata fileMetadata = new DfsMetadata();
		fileMetadata.setName(dirFileNames[pathLength-1]);
		fileMetadata.setUser(username);
		//determine which nodes to add blocks to
		//sending 1 block to each node according to replication factor (not sending multiple blocks to a node like Hadoop
		//TODO: remove the comments below
		Map<Integer, List<String>> blocks = fileMetadata.getBlocks();
		for(int i=1; i<=numBlocks; i++) {
			//block i of numBlocks
    		List<String> blocksAssigned = getKNodes();	//get K=replication factor number of nodes to send this block to
    		blocks.put(i, blocksAssigned);
    		//now increment the count of the number of blocks on the K datanodes by 1
    		//this should ensure even distribution of blocks depending on new loads on the datanodes
    		for(String dataNodeName: blocksAssigned) {
    			int count = _dataNodeLoad.get(dataNodeName);
    			_dataNodeLoad.put(dataNodeName, count+1);
    			//also add this block to the datanodeMap
    			
    		}
		}
		fileMetadata.setBlocks(blocks);
		parent.getFilesInDir().put(dirFileNames[pathLength-1], fileMetadata);
		
		//TODO: start from here after starting from the todo above: make the DatanodeMetadata for this file
		
		
		//return datanode names where the file blocks should be stored
//		return blocks;		
		return null;
    	
    }
    
    /**
     * 
     * @param path
     * @param username
     * @return
     * @throws RemoteException
     */
    public synchronized boolean deleteFileFromDfs(String path, String username) throws RemoteException {
    	if(!checkPathValidity(path, username)) {
    		return false;
    	}    	
    	String[] dirFileNames = path.split("/");
    	int pathLength = dirFileNames.length;
    	DfsStruct parent = _rootStruct.getSubDirsMap().get(username);
    	for(int i=3; i<pathLength-1; i++) {
    		if(parent.getSubDirsMap().containsKey(dirFileNames[i])) {
    			//keep going till the end to create the directory structure    			
    			parent = parent.getSubDirsMap().get(dirFileNames[i]);
    		} else {
    			//cannot have a directory name ending with .txt
    			throw new DfsFileNotFound();
    		} 
    	}
    	//now, if the file already exists, throw an exception such that it needs to be deleted first
    	if(parent.getFilesInDir().containsKey(dirFileNames[pathLength-1])) {
    		//file exists
    		
    	} else {
    		//file doesn't exist
    		throw new DfsFileNotFound();
    	}    	 
    	return true;
    }
    
    /**
     * Returns K (replication factor) data nodes with minimum load (in terms of disk space)
     * @return Data nodes with minimum load.
     */
    private synchronized List<String> getKNodes() {
    	Map<String, Integer> map = new TreeMap<String, Integer>(new LoadComparator(_dataNodeLoad));
    	map.putAll(_dataNodeLoad);
    	List<String> kNodes = new ArrayList<String>();
    	int k=0;
    	boolean repeat = true;
    	while(repeat) {
    		//if replication factor is bigger than the number of datanodes, we have to repeat nodes
    		//TODO: keep track of node capacity - part of cool stuff
	    	for(Entry<String, Integer> entry: map.entrySet()) {
	    		kNodes.add(entry.getKey());
	    		k++;
	    		if(k==_repFactor) {
	    			repeat = false;
	    			break;
	    		} 	    			    			
	    	}
    	}
    	return kNodes;
    }
    
    public synchronized void printDfsStructure() {
    	//TODO: send this to the user in the form of a file
    	Deque<DfsStruct> Q = new ArrayDeque<DfsStruct>();
    	Q.addLast(_rootStruct);
    	while(Q.size() >= 1) {
    		DfsStruct node = Q.removeFirst();
    		System.out.print(node.getPath()+": ");
    		Map<String, DfsStruct> subDirMap = node.getSubDirsMap();
    		//print all subdirs
    		for(Entry<String, DfsStruct> dir: subDirMap.entrySet()) {
    			System.out.print(dir.getKey()+" ");
    			Q.addLast(dir.getValue());
    		}
    		//print all files
    		Map<String, DfsMetadata> fileMap = node.getFilesInDir();
    		for(Entry<String, DfsMetadata> file : fileMap.entrySet()) {
    			System.out.print(file.getKey()+" ");
    		}
    		System.out.println();
    	}
    }
    
    private class LoadComparator implements Comparator<String> {
        private Map<String, Integer> dataNodeMap;        
        public LoadComparator(Map<String, Integer> map) {
            dataNodeMap = map;
        }        
        public int compare(String key1, String key2)
        {
            return dataNodeMap.get(key1) >= dataNodeMap.get(key2) ? 1 : -1;            
        }        
    }
    
    
    
    public static void main(String[] args) {   
    	DfsService_Impl dfsMain = new DfsService_Impl();
        //read config file and set corresponding values; also initialize the root directory of DFS
        dfsMain.dfsInit();
        try {
			dfsMain.addFileToDfs("/dfs/user1/file/a.txt", "user1", 3);
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			dfsMain.addFileToDfs("/dfs/user1/file/b.txt", "user1", 3);
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			dfsMain.addFileToDfs("/dfs/user2/file/c.txt", "user2", 3);
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			dfsMain.addFileToDfs("/dfs/user1/newfile/x.txt", "user1", 3);
			dfsMain.printDfsStructure();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		
        
        //now listen to requests from ClientAPI's and datanodes
//        ServerSocket serverSocket = null;
//        try {
//            serverSocket = new ServerSocket(dfsMain.nameNodePort);
//        }
//        catch (IOException e) {
//            System.out.println("EXCEPTION: Problem creating server socket on namenode. Program exiting.");
//            System.exit(0);
//        }
//        while(true) {
//            Socket newConnection = null;
//            try {
//                newConnection = serverSocket.accept();
//            }
//            catch (IOException e) {
//                System.out.println("EXCEPTION: Problem accepting connection on server socket on namenode.");
//                continue;
//            }
//            //process request in this thread itself
//            //TODO: START FROM HERE -- request types could be: put file in dfs + create folder + send nodenames to store data, 
//            //inpath/outpath validity, create folders for outpath, check path name validity (part 
//            //of previous three), notice from JobTracker about node going down->replicate
//            ObjectInputStream inStream;
//            ObjectOutputStream outStream;
//            String command;
//            try {
//                inStream = new ObjectInputStream(newConnection.getInputStream());
//                outStream = new ObjectOutputStream(newConnection.getOutputStream());
//                command = (String) inStream.readObject();
//            }
//            catch (IOException e) {
//                System.out.println("EXCEPTION: Problem reading from input stream on network.");
//                continue;
//            }
//            catch (ClassNotFoundException e) {
//                System.out.println("EXCEPTION: Problem reading correct input format on network.");
//                continue;
//            }            
//            dfsMain.handleRequests(command, inStream, outStream);
//        }
        
    }
}
