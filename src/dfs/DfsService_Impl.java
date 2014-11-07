package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import dfs.exceptions.DuplicateFileException;
import dfs.exceptions.EndsWithException;

public class DfsService_Impl implements DfsService {
	
final String _dfsPathIndentifier = "/dfs/";    //every path on dfs should start with this
    
    private Map<String, Integer> _dataNodeLoad;     //maintains a map between datanode and load on it in terms of number of blocks stored on it
    private int _repFactor;                         //replication factor
    private String[] _dataNodeNames;                //list of datanode names
    private int _dataNodesNum;                      //total number of datanodes
    private DfsStruct _rootStruct;                  //the root of the trie which represents the directory structure
    private int _nameNodePort;                      //port that namenode listens to
        
    private enum ConfigFileKeys{
    	TotalDataNodes,
    	DataNodeNames,
    	ReplicationFactor,
    	NameNodePort
    }
    
    public void dfsInit() {
        //initialize data node map with initial load of 0
        FileReader fr = null;
        try {
            fr = new FileReader("src/dfs/tempDfsConfigFile");
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
                    default: {
                        System.out.println("Unrecognized key in config file: " + keyValue[0]);
                        break;
                    }
                }
            }
            
            //Initialize the trie for storing DFS and corresponding local paths
            //Step 1: create the root node
            _rootStruct = new DfsStruct("dfs", "/dfs/");
            
        }
        catch (FileNotFoundException e) {
            System.out.println("EXCEPTION: Config file not found.");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Config file IO exception.");
            System.exit(0);
        }
        
        _dataNodeLoad = new HashMap<String, Integer>();
        for(String dataNode: _dataNodeNames) {
            System.out.println(dataNode);
            _dataNodeLoad.put(dataNode, 0);
        }        
        //TODO: Use this for sending list of datanodes to distribute the file on
//        Map<String, Integer> map = new TreeMap<String, Integer>(new LoadComparator(dataNodeLoad));
//        map.putAll(dataNodeLoad);
//        
//        for(Entry<String, Integer>entry: map.entrySet()) {
//            System.out.println(entry.getKey()+"-"+entry.getValue());
//        }
    }
    
    /**
     * Checks the validity of the file path provided by the user.
     * @param path Path for a file provided by user.
     * @param username Username of the user 
     * @return Whether the path is valid (Boolean).
     */
    public boolean checkPathValidity(String path, String username) {
        if(!path.startsWith("/dfs/"+username+"/") || !path.endsWith(".txt")) {
        	//user cannot add directories without adding files
        	return false;
        }        	         
        return true;
    }
    
    /**
     * Checks if a particular file exists in the user's directory.
     * @param path The path to the file.
     * @param username The username of the user trying to access the file.
     * @return Whether the file exists in user's subdirectory on DFS.
     */
    public boolean checkFileExists(String path, String username) {
    	if(!checkPathValidity(path, username)) {
    		return false;
    	}
    	String[] dirFileNames = path.split("/");
    	DfsStruct tempStruct = _rootStruct;
    	//dirFileNames[0] == "", so we start at index 1
    	for(int i=1; i<dirFileNames.length-1; i++) {
    		//check if the directory structure matches the path
    		if(!tempStruct.getName().equals(dirFileNames[i])) {
    			return false;
    		}
    		tempStruct = tempStruct.getSubDirsMap().get(dirFileNames[i]);
    	}
    	if(tempStruct.getFilesInDir().containsKey(dirFileNames[dirFileNames.length-1])) {
    		return true;
    	}
    	return false;
    }
    
    public boolean addFileToDfs(String path, String username) throws RemoteException {
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
    		} else if(dirFileNames[i].endsWith(".txt")) {
    			//cannot have a directory name ending with .txt
    			throw new EndsWithException("Directory name cannot contain .txt");
    		} else {
    			//directory name valid, but directory does not exist, so create directory
    			DfsStruct newStruct = new DfsStruct(dirFileNames[i]);
    			parent.getSubDirsMap().put(dirFileNames[i], newStruct);
    			parent = newStruct;
    		}
    	}
    	//now, if the file already exists, throw an exception such that it needs to be deleted first
    	if(parent.getFilesInDir().containsKey(dirFileNames[pathLength-1])) {
    		//file already exists
    		throw new DuplicateFileException("File needs to be deleted before adding again.");
    	} else {
    		//add file to DFS
    		DfsMetadata fileMetadata = new DfsMetadata();
    		fileMetadata.setBlocks(new ArrayList<String>);
    		parent.getFilesInDir().put(dirFileNames[pathLength-1], new DfsMetadata());
    	}
    	 
    	
    	return false;
    }
    
    /**
     * Returns the data node with minimum load (in terms of disk space)
     * @return Data node with minimum load.
     */
    private String getMinLoad() {
        int min = Integer.MAX_VALUE;
        String minNode = null;
        for(Entry<String, Integer> entry : _dataNodeLoad.entrySet()) {
            if(entry.getValue() < min) {
                min = entry.getValue();
                minNode = entry.getKey();
            }
        }
        
        return minNode;
    }
    
//    private class LoadComparator implements Comparator<String> {
//        private Map<String, Integer> map;        
//        public LoadComparator(Map<String, Integer> map) {
//            _map = map;
//        }        
//        public int compare(String key1, String key2)
//        {
//            return map.get(key1) >= map.get(key2) ? 1 : -1;            
//        }        
//    }
    
    public static void main(String[] args) {   
        DfsMain dfsMain = new DfsMain();
        //read config file and set corresponding values; also initialize the root directory of DFS
        dfsMain.dfsInit();
        //TODO: spawn JobTracker
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
