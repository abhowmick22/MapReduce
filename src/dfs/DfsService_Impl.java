package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import datanode.Node;
import dfs.exceptions.InvalidPathException;

public class DfsService_Impl implements DfsService {
	
final String _dfsPathIndentifier = "/dfs/";    //every path on dfs should start with this
    
	//TODO: make everything synchornized and concurrent
    private int _repFactor;                         	//replication factor
    private Map<String, Boolean> _dataNodeNamesMap;     //map of currently active datanode names   
    private DfsStruct _rootStruct;                  	//the root of the trie which represents the directory structure
    int _registryPort;                          //port number for the registry
    private String _localBaseDir;						//base directory on the local file system of each datanode
    private int _dnRegistryPort;          //Datanode registry ports (for each datanode)
    private Map<String, Registry> _dnRegistries;         //handle for Datanode registries (for each datanode)
    private Map<String, Node> _dnServices;              //handle for Datanode services (for each datanode)
    
    
    //data structures to recover from datanode failure
    //TODO: DFS is not involved in this, although an update to DfsMetadata has to be made after block transfer to new node
    private Map<String, List<String>> _dataNodeBlockMap;		//map from datanode name to all the file blocks it stores
    private Map<String, List<String>> _fileBlockNodeMap;		//local block names and corresponding datanodes where they are saved    													
    
    /**
     * Initializes the DFS on the node from where it is run. It needs the config file for initialization.
     */
    public DfsService_Impl() {
        FileReader fr = null;
        try {
            //TODO: perform input checks
            fr = new FileReader("tempDfsConfigFile");	//TODO: change the name
            BufferedReader br = new BufferedReader(fr);            
            String line = "";
            while((line=br.readLine())!=null) {                
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if(key.equals("DataNodeNames")) {
                    String[] tempNodeNames = keyValue[1].split(",");
                    _dataNodeNamesMap = new ConcurrentHashMap<String, Boolean>();
                    _dnRegistries = new HashMap<String, Registry>();
                    _dnServices = new HashMap<String, Node>();
                    for(int i=0; i<tempNodeNames.length; i++) {
                        //remove whitespaces 
                        String nodename = tempNodeNames[i].replaceAll("\\s", "");
                        _dataNodeNamesMap.put(nodename, false);
                        _dnRegistries.put(nodename, null);
                        _dnServices.put(nodename, null);
                    }                        
                } else if (key.equals("ReplicationFactor")) {
                    _repFactor = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                    if(_repFactor <= 0) {
                        System.out.println("Replication factor should be at least 1. Program exiting...");
                        System.exit(0);
                    }                        
                } else if (key.equals("DFS-RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));                                            
                } else if(key.equals("DN-RegistryPort")) {                    
                    _dnRegistryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));                                                
                } else if(key.equals("LocalBaseDir")) {
                    _localBaseDir = keyValue[1].replaceAll("\\s", "");
                } else if(key.equals("DFS-RegistryHost")) {
                    //check if the registry was started on the correct node
                    String dfsRegistryHost = keyValue[1].replaceAll("\\s", "");
                    if(!dfsRegistryHost.equals(InetAddress.getLocalHost().getHostName())) {
                        System.out.println("Please start the DFS registry on the machine specified in the Config File.");
                        br.close();
                        System.exit(0);
                    }
                }                 
            }
            br.close();
            
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
        for(String dataNode: _dataNodeNamesMap.keySet()) {
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
     * @param overwrite Whether to overwrite the file if it already exists in the DFS.
     * @return A map of block names to the datanode names where the individual blocks should go, according to the replication factor.
     * @throws RemoteException
     */
    @Override
    public synchronized Map<String, List<String>> addFileToDfs(String path, String username, 
            int numBlocks, boolean overwrite) throws RemoteException {
    	if(!checkPathValidity(path, username)) {
    		//invalid path
    		throw new InvalidPathException();
    	}  
    	if(checkFileExists(path, username, true)) {
    		//file already exists
    	    if(!overwrite) {
    	        //throw new DuplicateFileException();
    	        return getDfsFileMetadata(path, username).getBlocks();
    	    } else {
    	        deleteFileFromDfs(path, username);
    	    }
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
		Map<String, Boolean> blockConfirm = fileMetadata.getBlockAndNodeNameConfirm();
		//create generic name for each block 
		String genericBlockName = username;
		for(int i=3; i<pathLength; i++) {
			//this will store the entire dfs path including username as the genericblockname
			genericBlockName = genericBlockName+"--"+dirFileNames[i];
		}
		for(int i=1; i<=numBlocks; i++) {
			//block i of numBlocks
			//create the filename for this block, as it will be stored on local file systems of datanode
			String blockName = genericBlockName+"--"+i;	//unique block name for each block of each file uploaded by a user
			//get K=replication factor number of nodes to send this block to
    		List<String> nodesAssigned = getKNodes();
    		System.out.print(blockName+": ");
    		for(String node: nodesAssigned) {
    		    System.out.print(node+", ");
    		}
    		System.out.println();
    		blocks.put(blockName, nodesAssigned);
    		//now add the block to the dataNodeBlockMap to the corresponding datanode
    		//this should ensure even distribution of blocks depending on new loads on the datanodes
    		//because the comparator is based on the number of blocks on each datanode
    		for(String dataNodeName: nodesAssigned) {
    			_dataNodeBlockMap.get(dataNodeName).add(blockName);
    			//also put this block+datanodes combination in blockConfirm
    			//to begin with, all are false. they become true when a datanode confirms the succesful receipt of a block
    			blockConfirm.put(blockName+"--"+dataNodeName, false);        		
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
     * The confirmation is in the form of a string of the form: <blockname>--<datanodename>, 
     * where datanodename is the name of that datanode.
     * @param blockAndNodeName The confirmation string from the description.
     */
    @Override
    public synchronized void confirmBlockAndNodeNameReceipt(String blockAndNodeName) throws RemoteException{
    	//TODO: retrieve the DFS file path of the block's file name from the block name
    	//then set the blockname-datanodename combo in the blockConfirm map of that DfsMetadata as true
    	//This is use only by data nodes to confirm receipt of the block
    	//TODO: when the user performs map reduce on a file, make sure all blocks are present on some data node at least
    	String[] nameString = blockAndNodeName.split("--");
    	//String nodeName = nameString[nameString.length-1];
    	String path = "/dfs/";
    	String username = nameString[0];
    	for(int i=0; i<nameString.length-3; i++) { //second last index of nameString has block number
    		path = path+nameString[i]+"/";
    	}
    	path = path+nameString[nameString.length-3];	//filename shouldn't be followed by "/"
    	getDfsFileMetadata(path, username).getBlockAndNodeNameConfirm().put(blockAndNodeName, true);
    }
    
    /**
     * Deletes a file from the DFS.
     * @param path The path to the file on DFS.
     * @param username The username of the user who "ordered" the delete.
     * @return The map of block names to the list of machines that each block is assigned so the ClientAPI can delete the file blocks.
     * @throws RemoteException
     */    
    @Override
    public synchronized void deleteFileFromDfs(String path, String username) throws RemoteException {
    	//TODO: do all the deletion from here itself, rather than sending back to client api to do the deletion
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
    	Map<String, Boolean> blockAndNodeNameConfirm = dfsFileMetadata.getBlockAndNodeNameConfirm();
    	for(Entry<String, List<String>> entry: blocks.entrySet()) {
    		//key: block name
    		//value: list of datanodes on which this block is supposed to reside
    		//remove entry from the block to node map 
    	    String blockName = entry.getKey();
    		_fileBlockNodeMap.remove(blockName);
    		List<String> dataNodeList = entry.getValue();
    		//create new temp list for iterating
    		List<String> tempList = new ArrayList<String>(dataNodeList);
    		for(String dataNode: tempList) {
    			String blockAndNodeName = blockName+"--"+dataNode;
    			if(!blockAndNodeNameConfirm.get(blockAndNodeName)) {
    				dataNodeList.remove(dataNode);      //remove those datanodes from this block for which we never got
    				                                    //a confirmation of block receipt    				
    			}
    			//also remove the block from the data node to block map
    			_dataNodeBlockMap.get(dataNode).remove(blockName);
    		}
    		//remove file from all datanodes that contain the block
    		for(String dataNode: dataNodeList) {
    		    System.out.println(dataNode);
    		    if(_dnServices.get(dataNode) == null) {
    		        //datanode down
    		        continue;
    		    }
    		    try {
    		        _dnServices.get(dataNode).deleteFile(_localBaseDir+blockName);
    		    }
    		    catch(RemoteException e) {
    		        System.out.println("Remote Exception: Could not delete block "+blockName+" from datanode "+dataNode+
    		                " (probably) because the datanode failed, or there was an IO exception at the datanode.");
    		    }
    		}
    	}	
    	//remove reference of the dfsFileMetadata from the parentStruct, hence the directory
    	//does not have a reference to this file any more
    	parentStruct.getFilesInDir().remove(fileName);    	
    }
    
    @Override
    public synchronized Map<String, List<String>> getFileFromDfs(String dfsPath, String username) throws RemoteException {
        if(!checkPathValidity(dfsPath, username)) {
            throw new InvalidPathException();
        }
        //get the DfsFileMetadata of this file
        DfsFileMetadata dfsFileMetadata = getDfsFileMetadata(dfsPath, username);
        Map<String, List<String>> blocks = dfsFileMetadata.getBlocks();
        Map<String, Boolean> blockAndNodeNameConfirm = dfsFileMetadata.getBlockAndNodeNameConfirm();
        Map<String, List<String>> retMap = new HashMap<String, List<String>>();
        for(Entry<String, List<String>> entry: blocks.entrySet()) {
            //key: block name
            //value: list of datanodes on which this block is supposed to reside            
            List<String> dataNodeList = entry.getValue();
            //create new temp list for returning only those nodenames that confirmed the receipt of this block
            List<String> tempList = new ArrayList<String>();
            for(String dataNode: dataNodeList) {
                String blockAndNodeName = entry.getKey()+"--"+dataNode;
                if(blockAndNodeNameConfirm.get(blockAndNodeName) && _dataNodeNamesMap.get(dataNode)) {
                    //send only those node names which are currently available and contain the block
                    tempList.add(dataNode);
                }                
            }
            retMap.put(entry.getKey(), tempList);
        }   
        
        return retMap;       
    }
    
    @Override
    public synchronized void updateActiveNodes(List<String> activeNodeList, String nodeListSentBy) throws RemoteException {
        Set<String> keySet = _dataNodeNamesMap.keySet();    //this contains an exhaustive list of all nodes that can be running
                                                            //because this map was created from the config file        
        List<String> failedNodes = new ArrayList<String>();
        for(String nodename: keySet) {
            if(activeNodeList.contains(nodename)) {
                _dataNodeNamesMap.put(nodename, true);
                System.out.println(nodename+" was added.");
                //update reference to its registry and datanode service if null before
                if(_dnRegistries.get(nodename) == null || _dnServices.get(nodename) == null) {
                    try {            
                        _dnRegistries.put(nodename, 
                                LocateRegistry.getRegistry(nodename, _dnRegistryPort));
                        _dnServices.put(nodename, 
                                    (Node) _dnRegistries.get(nodename).lookup("DataNode"));                        
                    }             
                    catch (RemoteException e) {
                        //set the datanode registry and service to null for this node, and make it unavailable
                        System.out.println("Remote Exception. Datanode "+nodename+" not accessible.");                        
                        _dataNodeNamesMap.put(nodename, false);
                        _dnRegistries.put(nodename, null);
                        _dnServices.put(nodename, null);
                        failedNodes.add(nodename);                          
                    }
                    catch (NotBoundException e) {
                        //set the datanode registry and service to null for this node, and make it unavailable
                        System.out.println("Registry not bound. Datanode "+nodename+" not accessible.");                        
                        _dataNodeNamesMap.put(nodename, false);
                        _dnRegistries.put(nodename, null);
                        _dnServices.put(nodename, null);
                        failedNodes.add(nodename);    
                    }
                }
            } else {
                try {
                    if(!nodeListSentBy.equals(InetAddress.getLocalHost().getHostName()) ||
                            _dataNodeNamesMap.containsKey(nodename) && !_dataNodeNamesMap.get(nodename)) {
                        //node list not sent by job tracker, but by a datanode that just got activated
                        //or the datanode had failed previously, and has not been back up since
                        continue;
                    }
                }
                catch (UnknownHostException e1) {
                    System.out.println("Unknown HostException:");
                    System.out.println(e1.getMessage());
                    continue;
                }                
                _dataNodeNamesMap.put(nodename, false);
                //also make registry and service entries null, so when the node comes back, we can connect again
                //to the new registry and service on that node
                _dnRegistries.put(nodename, null);
                _dnServices.put(nodename, null);
                failedNodes.add(nodename);                
            }
        }
        //carry out procedure to maintain replication 
        if(failedNodes.size() > 0) {
            transferFilesBetweenNodes(failedNodes);
        }
    }
    
    @Override
    public synchronized void reportFailedNode(final String nodename, final String dfsPath, 
            final String username, final boolean removeFile) throws RemoteException {
        _dataNodeNamesMap.put(nodename, false);
        _dnRegistries.put(nodename, null);
        _dnServices.put(nodename, null);
        //create new thread to call transferFilesBetweenNodes method so that 
        //the clientapi that called this method is not blocked
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized(this) {
                    List<String> failedNodes = new ArrayList<String>();
                    failedNodes.add(nodename);
                    //take care of the failed node, and transfer blocks on it to maintain replication factor                
                    transferFilesBetweenNodes(failedNodes);
                    //Now, delete the file if the removeFile variable is set. If it is set, then
                    //the datanode failed during a file add operation, and the file should be deleted
                    //so that if the user adds the file again, we do not return the same datanode names                    
                    if(removeFile) {
                        try {                            
                            deleteFileFromDfs(dfsPath, username);
                        }
                        catch (RemoteException e) {
                            System.out.println("Failed to delete file: " + dfsPath);
                        }
                    }
                }
            }
        }).start();        
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
    
    @Override
    public List<String> getBlocksOnNode(String nodename) {
        if(!_dataNodeBlockMap.containsKey(nodename)) {
            return null;
        }
        return _dataNodeBlockMap.get(nodename);
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
                //add to kNodes only if the node is active
                if(!_dataNodeNamesMap.get(key)) {
                    continue;
                }
                System.out.println("node: "+key);
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
    
//    /**
//     * Returns one node to which a block from another node should be transferred to maintain the
//     * replication factor.
//     * @param nodeListToAvoid List of nodes that already possess the given block.
//     * @return Data node with minimum load that does not contain a particular block
//     */
//    private synchronized List<String> getRepNode(List<String> nodeListToAvoid) {
//        Map<String, List<String>> map = new TreeMap<String, List<String>>(new LoadComparator(_dataNodeBlockMap));
//        map.putAll(_dataNodeBlockMap);
//        List<String> kNodes = new ArrayList<String>();
//        int k=0;
//        boolean repeat = true;
//        while(repeat) {
//            //if replication factor is bigger than the number of datanodes, we have to repeat nodes
//            //TODO: keep track of node capacity - part of cool stuff
//            for(String key: map.keySet()) {
//                //add to kNodes only if the node is active
//                if(!_dataNodeNamesMap.get(key)) {
//                    continue;
//                }
//                kNodes.add(key);
//                k++;
//                if(k==_repFactor) {
//                    repeat = false;
//                    break;
//                }                                   
//            }
//        }
//        return kNodes;
//    }
    
    /**
     * Used to maintain replication factor by transferring all the file blocks that are part of a
     * node that went down, from nodes that have the same block to one more node that is chosen
     * based on the load factor.
     * @param dataNodeName The name of the node that failed.
     */
    private synchronized void transferFilesBetweenNodes(List<String> failedNodes) {
        for(String node: failedNodes) {
            List<String> fileBlockNames = _dataNodeBlockMap.get(node);
            if(fileBlockNames == null) {
                System.out.println("No blocks on failed node: "+node);
                continue;
            }            
            //look for an alternate node that has the same block
            DfsFileMetadata fileMetadata = null;
            for(String fileBlock: fileBlockNames) {
                List<String> allNodesContainingThisBlock = _fileBlockNodeMap.get(fileBlock);
                //select one to transfer from
                String alternateNode = null;
                for(String alternatePossibleNode: allNodesContainingThisBlock) {
                    String blockAndNodeName = fileBlock+"--"+alternatePossibleNode;                    
                    System.out.println(blockAndNodeName);
                    String[] pathArray = fileBlock.split("--");
                    String path = "/dfs/";
                    for(int i=0; i<pathArray.length-1;i++) {
                        path = path + pathArray[i] + "/";
                    }
                    fileMetadata = getDfsFileMetadata(path, pathArray[0]);  //because pathArray[0] is the username
                    System.out.println(path+", "+pathArray[0]);
                    if(alternatePossibleNode.equals(node) || 
                            !fileMetadata.getBlockAndNodeNameConfirm().get(blockAndNodeName)) {
                        //The two nodes are the same or DFS didn't get a confirmation about this node receiving the block
                        continue;
                    }
                    alternateNode = alternatePossibleNode;
                    break;
                }
                if(alternateNode == null) {
                    //didn't find any node to replicate to
                    System.out.println("Cannot replicate the block \""+fileBlock+"\" after the datanode \""+node+"\" went down.");
                    fileMetadata = null;
                    continue;
                }                                 
                //TODO: ideally we'd want to send the block to a node that doesn't already have it,
                //but we're not doing that now. For now, we just send it to the one with min load
                String newNode = getKNodes().get(0);
                //add new node to all the datastructures that contain a reference to the lost block
                fileMetadata.getBlocks().get(fileBlock).add(newNode);
                _dataNodeBlockMap.get(newNode).add(fileBlock);
                _fileBlockNodeMap.get(fileBlock).add(newNode);
                fileMetadata.getBlockAndNodeNameConfirm().put(fileBlock+"--"+newNode, false);                
                try {
                    //even if this fails, we add the newNode to the above data structures
                    //because if this fails then the getBlockAndNodeNameConfirm() method
                    //will never be called and we won't consider the newNode for the given block (fileBlock) anyway.
                    if(_dnServices.get(alternateNode).transferBlockTo(_dnServices.get(newNode), 
                            _localBaseDir+fileBlock)) {
                        fileMetadata.getBlockAndNodeNameConfirm().put(fileBlock+"--"+newNode, true);
                    }                    
                }
                catch (RemoteException e) {
                    System.out.println("Problem replicating the block \""+fileBlock+"\" after the datanode \""+node+"\" went down.");
                    continue;
                }             
            }            
        }                 
        
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
//    	DfsService_Impl dfsMain = new DfsService_Impl();
//        if (System.getSecurityManager() == null) {
//
//            System.setProperty("java.security.policy", "server.policy"); 
//            System.setSecurityManager(new SecurityManager());           
//            
//        }
//        //read config file and set corresponding values; also initialize the root directory of DFS        
//        dfsMain.dfsInit();
//        try {
//            String name = "DfsService";
//            DfsService service = new DfsService_Impl();
//            DfsService stub =
//                (DfsService) UnicastRemoteObject.exportObject(service, 0);
//            Registry registry = LocateRegistry.createRegistry(dfsMain._registryPort);
//            registry.rebind(name, stub);
//            System.out.println(registry.list().length);
//            System.out.println("DfsService bound");
//        } catch (Exception e) {
//            System.err.println("DfsService exception:");
//            e.printStackTrace();
//        }
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
			dfsMain.confirmBlockReceipt("user1-file-b.txt-1"+"--"+datanodes.get("user1-file-b.txt-1").get(0));
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
