package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import dfs.exceptions.DuplicateFileException;
import dfs.exceptions.InvalidPathException;

public class DfsService_Impl implements DfsService {
	
final String _dfsPathIndentifier = "/dfs/";    //every path on dfs should start with this
    
	//TODO: make everything synchornized and concurrent
    private int _repFactor;                         	//replication factor
    private String[] _dataNodeNames;                	//list of datanode names
    private DfsStruct _rootStruct;                  	//the root of the trie which represents the directory structure
    int _registryPort;                          //port number for the registry
//    private int _nameNodePort;                      	//port that namenode listens to
//    private String _localBaseDir;						//base directory on the local file system of each datanode
    
    //data structures to recover from datanode failure
    //TODO: DFS is not involved in this, although an update to DfsMetadata has to be made after block transfer to new node
    private Map<String, List<String>> _dataNodeBlockMap;		//map from datanode name to all the file blocks it stores
    private Map<String, List<String>> _fileBlockNodeMap;		//local block names and corresponding datanodes where they are saved
    													
        
    private enum ConfigFileKeys{
    	DataNodeNames,
    	ReplicationFactor,
    	NameNodePort,
    	LocalBaseDir,
    	RegistryPort
    }
    
    /**
     * Initializes the DFS on the node from where it is run. It needs the configfile for initialization.
     */
    void dfsInit() {
        FileReader fr = null;
        try {
            fr = new FileReader("tempDfsConfigFile");	//TODO: change the name
            BufferedReader br = new BufferedReader(fr);            
            String line;
            while((line=br.readLine())!=null) {                
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if(key.equals("DataNodeNames")) {
                    _dataNodeNames = keyValue[1].split(",");
                    //remove whitespaces
                    for(int i=0; i<_dataNodeNames.length; i++) {
                        _dataNodeNames[i] = _dataNodeNames[i].replaceAll("\\s", "");
                    }                        
                } else if (key.equals("ReplicationFactor")) {
                    _repFactor = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                    if(_repFactor <= 0) {
                        System.out.println("Replication factor should be at least 1. Program exiting...");
                        System.exit(0);
                    }                        
                } else if (key.equals("RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));                                            
                }
//                    case NameNodePort: {
//                        _nameNodePort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
//                        break;
//                    }
//                    case LocalBaseDir: {
//                      _localBaseDir = keyValue[1].replaceAll("\\s", "");
//                      break;
//                    }
            }
            
            //Initialize the trie for storing DFS and corresponding local paths
            //Step 1: create the root node
            _rootStruct = new DfsStruct("dfs", "/dfs/");
            _fileBlockNodeMap = new ConcurrentHashMap<String, List<String>>();
            
        }
        catch (FileNotFoundException e) {
            System.out.println("EXCEPTION: Config file not found.");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Config file IO exception.");
            System.exit(0);
        }
        
        _dataNodeBlockMap = new ConcurrentHashMap<String, List<String>>();
        for(String dataNode: _dataNodeNames) {
            _dataNodeBlockMap.put(dataNode, new ArrayList<String>());
        }        
        
    }
    
    /**
     * Checks the validity of the file path provided by the user.
     * @param path Path for a file provided by user.
     * @param username Username of the user 
     * @return Whether the path is valid (Boolean).
     */
    private synchronized boolean checkPathValidity(String path, String username) {
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
    private synchronized boolean checkFileExists(String path, String username, boolean skipPathValidityTest) throws RemoteException {
    	if(!skipPathValidityTest) {
    		if(!checkPathValidity(path, username)) {
        		throw new InvalidPathException();
    		}
    	}
    	if(getDfsFileMetadata(path, username) == null)
    		return false;
    	return true;
    }
    
    /**
     * Get the metadata associated with a file stored on the DFS.
     * @param path The path to the file on DFS.
     * @param username The username of the user that created the file.
     * @return The metadata structure DfsFileMetadata associated with this file.
     */
    private synchronized DfsFileMetadata getDfsFileMetadata(String path, String username) {
    	//TODO: don't think there is need to check path validity as it is called from methods
    	//which are sure of the path being valid, but think about this at the end
    	String[] dirFileNames = path.split("/");
    	DfsStruct tempStruct = _rootStruct.getSubDirsMap().get(username);
    	//dirFileNames[0] == "", so we start at index 1
    	for(int i=3; i<dirFileNames.length-1; i++) {
    		//check if the directory structure matches the path
    		if(tempStruct == null || !tempStruct.getSubDirsMap().containsKey(dirFileNames[i])) {
    			return null;
    		}
    		tempStruct = tempStruct.getSubDirsMap().get(dirFileNames[i]);
    	}
    	if(tempStruct != null && tempStruct.getFilesInDir().containsKey(dirFileNames[dirFileNames.length-1])) {
    		return tempStruct.getFilesInDir().get(dirFileNames[dirFileNames.length-1]);
    	}
    	return null;
    }
    
//    /**
//     * Returns the DfsStruct associated with the last directory in the path.
//     * @param path The path of the directory on DFS.
//     * @return The DfsStruct associated with the path.
//     */
//    private synchronized DfsStruct getDfsStruct(String path) {
//    	//TODO: don't think there is need to check path validity as it is called from methods
//    	//which are sure of the path being valid, but think about this at the end
//    	String[] dirFileNames = path.split("/");
//    	DfsStruct tempStruct = _rootStruct;
//    	//Note: dirFileNames[0] == "", so we start at index 1
//    	for(int i=2; i<dirFileNames.length; i++) {
//    		//start with username
//    		tempStruct = tempStruct.getSubDirsMap().get(dirFileNames[i]);
//    	}
//    	return tempStruct;
//    }
    
    /**
     * Adds a file given by the user to the DFS.
     * @param path The DFS path of the file.
     * @param username The username of the user adding the file.
     * @param numBlocks Expected number of blocks that the file will be divided into. The expected number of blocks is used because
     * we do not perform division of files into blocks before knowing where these blocks will have to be sent.
     * @return A map of block names to the datanode names where the individual blocks should go, according to the replication factor.
     * @throws RemoteException
     */
    @Override
    public synchronized Map<String, List<String>> addFileToDfs(String path, String username, int numBlocks) throws RemoteException {
    	if(!checkPathValidity(path, username)) {
    		//invalid path
    		throw new InvalidPathException();
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
    	DfsStruct parentStruct = _rootStruct.getSubDirsMap().get(username);
    	//now traverse the directory structure till the filename is expected to be encountered (i.e. pathLength-1)
    	for(int i=3; i<pathLength-1; i++) {
    		if(parentStruct.getSubDirsMap().containsKey(dirFileNames[i])) {
    			//keep going till the end to create the directory structure    			
    			parentStruct = parentStruct.getSubDirsMap().get(dirFileNames[i]);
    		} else {
    			//directory name valid, but directory does not exist, so create directory
    			addPath = addPath + dirFileNames[i] + "/";
    			DfsStruct newStruct = new DfsStruct(dirFileNames[i], addPath);
    			parentStruct.getSubDirsMap().put(dirFileNames[i], newStruct);
    			parentStruct = newStruct;
    		}
    	}
    	
    	//add file to DFS
		DfsFileMetadata fileMetadata = new DfsFileMetadata();
		fileMetadata.setName(dirFileNames[pathLength-1]);
		fileMetadata.setUser(username);
		fileMetadata.setParentDfsStruct(parentStruct);
		
		//Now, determine which nodes to add blocks to
		//sending 1 block to each node according to replication factor (not sending multiple blocks to a node like Hadoop
		Map<String, List<String>> blocks = fileMetadata.getBlocks();
		//confirmation map
		Map<String, Boolean> blockConfirm = fileMetadata.getBlockConfirm();
		//create generic name for each block 
		String genericBlockName = username;
		for(int i=3; i<pathLength; i++) {
			//this will store the entire dfs path including username as the genericblockname
			genericBlockName = genericBlockName+"-"+dirFileNames[i];
		}
		for(int i=1; i<=numBlocks; i++) {
			//block i of numBlocks
			//create the filename for this block, as it will be stored on local file systems of datanode
			String blockName = genericBlockName+"-"+i;	//unique block name for each block of each file uploaded by a user
			//get K=replication factor number of nodes to send this block to
    		List<String> nodesAssigned = getKNodes();	
    		blocks.put(blockName, nodesAssigned);
    		//now add the block to the dataNodeBlockMap to the corresponding datanode
    		//this should ensure even distribution of blocks depending on new loads on the datanodes
    		//because the comparator is based on the number of blocks on each datanode
    		for(String dataNodeName: nodesAssigned) {
    			_dataNodeBlockMap.get(dataNodeName).add(blockName);
    			//also put this block+datanodes combination in blockConfirm
    			//to begin with, all are false. they become true when a datanode confirms the succesful receipt of a block
    			blockConfirm.put(blockName+"-"+dataNodeName, false);        		
    		}
    		//we add a bit of redundancy here, in that we also add the node names for 
    		//every block, which is the opposite of above. This is done for easiness during
    		//determining the datanode to get the block from in case another datanode with this block fails
    		_fileBlockNodeMap.put(blockName, nodesAssigned);
    		
		}
		//fileMetadata.setBlocks(blocks);
		parentStruct.getFilesInDir().put(dirFileNames[pathLength-1], fileMetadata);
		
		//return datanode names where the file blocks should be stored
		return blocks;		
    	
    }
    
    /**
     * When a datanode receives a block of a particular file, it sends the confirmation to the DFS.
     * The confirmation is in the form of a string of the form: <blockname>-<datanodename>, 
     * where datanodename is the name of that datanode.
     * @param blockAndNodeName The confirmation string form the description.
     */
    @Override
    public synchronized void confirmBlockReceipt(String blockAndNodeName) throws RemoteException{
    	//TODO: retrieve the DFS file path of the block's file name from the block name
    	//then set the blockname-datanodename combo in the blockConfirm map of that DfsMetadata as true
    	//This is use only by data nodes to confirm receipt of the block
    	//TODO: when the user performs map reduce on a file, make sure all blocks are present on some data node at least
    	String[] nameString = blockAndNodeName.split("-");
    	//String nodeName = nameString[nameString.length-1];
    	String path = "/dfs/";
    	String username = nameString[0];
    	for(int i=0; i<nameString.length-3; i++) { //second last index of nameString has block number
    		path = path+nameString[i]+"/";
    	}
    	path = path+nameString[nameString.length-3];	//filename shouldn't be followed by "/"
    	getDfsFileMetadata(path, username).getBlockConfirm().put(blockAndNodeName, true);
    }
    
    /**
     * Deletes a file from the DFS.
     * @param path The path to the file on DFS.
     * @param username The username of the user who "ordered" the delete.
     * @return The map of block names to the list of machines that each block is assigned so the ClientAPI can delete the file blocks.
     * @throws RemoteException
     */
    @Override
    public synchronized Map<String, List<String>> deleteFileFromDfs(String path, String username) throws RemoteException {
    	if(!checkPathValidity(path, username)) {
    		throw new InvalidPathException();
    	}
    	//get the DfsFileMetadata of this file
    	DfsFileMetadata dfsFileMetadata = getDfsFileMetadata(path, username);
    	String fileName = dfsFileMetadata.getName();
    	//get parent DfsStruct of the file
    	DfsStruct parentStruct = dfsFileMetadata.getParentDfsStruct();
    	//before sending the datanodes from where these blocks need to be deleted,
    	//we remove all datanodes that never confirmed receiving a block they were
    	//supposed to receive
    	Map<String, List<String>> blocks = dfsFileMetadata.getBlocks();
    	Map<String, Boolean> blockConfirm = dfsFileMetadata.getBlockConfirm();
    	for(Entry<String, List<String>> entry: blocks.entrySet()) {
    		//key: block name
    		//value: list of datanodes on which this block is supposed to reside
    		//remove entry from the block to node map global variable
    		_fileBlockNodeMap.remove(entry.getKey());
    		List<String> dataNodeList = entry.getValue();
    		//create new temp list for iterating
    		List<String> tempList = new ArrayList<String>(dataNodeList);
    		for(String dataNode: tempList) {
    			String blockAndNodeName = entry.getKey()+"-"+dataNode;
    			if(!blockConfirm.get(blockAndNodeName)) {
    				dataNodeList.remove(dataNode);
    			}
    			//also remove the block from the data node to block map global variable
    			_dataNodeBlockMap.get(dataNode).remove(entry.getKey());
    		}
    	}	
    	//remove reference of the dfsFileMetadata from the parentStruct, hence the directory
    	//does not have a reference to this file any more
    	parentStruct.getFilesInDir().remove(fileName);
    	
    	//return the remaining blocks to datanode map for client api to send the 
    	//signal to these datanodes to delete the corresponding blocks
    	return new HashMap<String, List<String>>(dfsFileMetadata.getBlocks());
    }
    
    @Override
    public synchronized void testMethod() throws RemoteException {
        System.out.println("Works");
    }
    
    /**
     * Returns K (replication factor) data nodes with minimum load (in terms of disk space)
     * @return Data nodes with minimum load.
     */
    private synchronized List<String> getKNodes() {
    	Map<String, List<String>> map = new TreeMap<String, List<String>>(new LoadComparator(_dataNodeBlockMap));
    	map.putAll(_dataNodeBlockMap);
    	List<String> kNodes = new ArrayList<String>();
    	int k=0;
    	boolean repeat = true;
    	while(repeat) {
    		//if replication factor is bigger than the number of datanodes, we have to repeat nodes
    		//TODO: keep track of node capacity - part of cool stuff
	    	for(String key: map.keySet()) {
	    		kNodes.add(key);
	    		k++;
	    		if(k==_repFactor) {
	    			repeat = false;
	    			break;
	    		} 	    			    			
	    	}
    	}
    	return kNodes;
    }
    
    /**
     * Prints the DFS structure as of the moment when called.
     */
    @Override
    public synchronized String printDfsStructure() throws RemoteException {
    	String DfsStructure = "";
    	Deque<DfsStruct> Q = new ArrayDeque<DfsStruct>();
    	Q.addLast(_rootStruct);
    	while(Q.size() >= 1) {
    		DfsStruct node = Q.removeFirst();
    		DfsStructure += node.getPath()+": ";
    		Map<String, DfsStruct> subDirMap = node.getSubDirsMap();
    		//print all subdirs
    		for(Entry<String, DfsStruct> dir: subDirMap.entrySet()) {
    			DfsStructure += dir.getKey()+" ";
    			Q.addLast(dir.getValue());
    		}
    		//print all files
    		Map<String, DfsFileMetadata> fileMap = node.getFilesInDir();
    		for(Entry<String, DfsFileMetadata> file : fileMap.entrySet()) {
    			DfsStructure += file.getKey()+" ";
    		}
    		DfsStructure += "\n";
    	}
    	return DfsStructure;
    }
    
    private class LoadComparator implements Comparator<String> {
        private Map<String, List<String>> dataNodeMap;        
        public LoadComparator(Map<String, List<String>> map) {
            dataNodeMap = map;
        }        
        public int compare(String key1, String key2)
        {
            return dataNodeMap.get(key1).size() >= dataNodeMap.get(key2).size() ? 1 : -1;            
        }        
    }
     
    public static void main(String[] args) throws Exception {   
    	DfsService_Impl dfsMain = new DfsService_Impl();
        
    	System.out.println(new BufferedReader(new FileReader("server.policy")).readLine());
        if (System.getSecurityManager() == null) {

            System.setProperty("java.security.policy", "server.policy"); 
            System.setSecurityManager(new SecurityManager());           
            
        }
        //read config file and set corresponding values; also initialize the root directory of DFS        
        dfsMain.dfsInit();
        try {
            String name = "DfsService";
            DfsService service = new DfsService_Impl();
            DfsService stub =
                (DfsService) UnicastRemoteObject.exportObject(service, 0);
            Registry registry = LocateRegistry.createRegistry(dfsMain._registryPort);
            registry.rebind(name, stub);
            System.out.println(registry.list().length);
            System.out.println("DfsService bound");
        } catch (Exception e) {
            System.err.println("DfsService exception:");
            e.printStackTrace();
        }
        /* TEST CODE for DFS
        try {
        	dfsMain.addFileToDfs("/dfs/user1/file/a.txt", "user1", 3);
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			Map<String, List<String>> datanodes = dfsMain.addFileToDfs("/dfs/user1/file/b.txt", "user1", 3);
			System.out.println("-----"+datanodes.hashCode());
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			dfsMain.addFileToDfs("/dfs/user2/file/c.txt", "user2", 3);
			//dfsMain.printDfsStructure();
			//System.out.println("---------");
			dfsMain.addFileToDfs("/dfs/user1/newfile/x.txt", "user1", 3);
			dfsMain.printDfsStructure();
			System.out.println("---------");
			dfsMain.confirmBlockReceipt("user1-file-b.txt-1"+"-"+datanodes.get("user1-file-b.txt-1").get(0));
			Map<String, List<String>> blocks = dfsMain.deleteFileFromDfs("/dfs/user1/file/b.txt", "user1");
			dfsMain.printDfsStructure();
			for(Entry<String, List<String>> entry: blocks.entrySet()) {
				if(entry.getValue().size()>0)
					System.out.println(entry.getKey()+": "+entry.getValue().get(0));
			}
			
			System.out.println("----------");
			for(Entry<String, List<String>> entry: dfsMain._dataNodeBlockMap.entrySet()) {
				System.out.print(entry.getKey()+": ");
				List<String> list = entry.getValue();
				for(String value: list) {
					System.out.print(value+", ");
				}
				System.out.println();
			}
			System.out.println("-------------------------");
			String temp = "";
			String temp2 = "";
			for(Entry<String, List<String>> entry: dfsMain._fileBlockNodeMap.entrySet()) {
				temp = entry.getKey();
				System.out.print(entry.getKey()+": ");
				List<String> list = entry.getValue();
				for(String value: list) {
					System.out.print(value+", ");
					temp2 = value;
				}
				System.out.println();
			}
			
			
		} catch (RemoteException e) {
			
			e.printStackTrace();
		}
			*/
		
    }
}
