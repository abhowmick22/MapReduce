package clientapi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import datanode.Node;
import dfs.DfsService;
import dfs.InputSplit;

public class ClientApi_Impl implements ClientApi {

    final int _blockSize = 2*1000*1000;        //TODO: put this somewhere else, also change size to 64MB. this is test size.
    final String _recordDelimiter = "\n";         //TODO: put this somewhere else, and use this to read records
    
	private int _dfsRegistryPort;			//DFS registry port 
	private String _dfsRegistryHost;       //DFS registry host
	private Registry _dfsRegistry;      //handle for DFS registry
	private DfsService _dfsService;     //handle for DFS service 
	private List<String> _dnRegistryHosts;          //Datanode registry hosts (for each datanode) -- same as dataNodeNames
	private int _dnRegistryPort;          //Datanode registry ports (for each datanode)
	private Map<String, Registry> _dnRegistries;         //handle for Datanode registries (for each datanode)
	private Map<String, Node> _dnServices;              //handle for Datanode services (for each datanode)
	private String _localBaseDir;                  //base directory on datanodes to store blocks
	private String _hostName;                  //hostname of the user
	
	//TODO: put all the path validity checks and the file exists checks in DFS/Namenode methods
	public ClientApi_Impl() {
		
		if (System.getSecurityManager() == null) {
		    System.setProperty("java.security.policy", "client.policy");
            System.setSecurityManager(new SecurityManager());
        }
		
		FileReader fr = null;
        try {
            fr = new FileReader("tempDfsConfigFile");   //TODO: change the name
            BufferedReader br = new BufferedReader(fr);
            _dfsRegistryPort = -1;
            _dfsRegistryHost = "";            
            String line;
            while((line=br.readLine())!=null) {  
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if(key.equals("DFS-RegistryPort")) {
                    _dfsRegistryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                } else if(key.equals("DFS-RegistryHost")) {
                    _dfsRegistryHost = keyValue[1].replaceAll("\\s", "");
                } else if(key.equals("DataNodeNames")) {
                    String[] tempRegistryHosts = keyValue[1].split(",");
                    //remove whitespaces
                    _dnRegistryHosts = new ArrayList<String>();
                    for(int i=0; i<tempRegistryHosts.length; i++) {
                        _dnRegistryHosts.add(tempRegistryHosts[i].replaceAll("\\s", ""));
                    }                        
                } else if(key.equals("DN-RegistryPort")) {                    
                    _dnRegistryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));                                                
                } else if(key.equals("LocalBaseDir")) {
                    _localBaseDir = keyValue[1].replaceAll("\\s", "");
                }         
            } 
            if(_dfsRegistryPort == -1 || _dfsRegistryHost.equals("")) {
                System.out.println("Registry port/host not found. Program exiting..");
                System.exit(0);
            } 
            br.close();
            
        } catch (Exception e) {
            System.err.println("DfsService exception:");
            System.out.println(e.getMessage());
        }
        
        //get DFS Service handle         
        try {
            _dfsRegistry = LocateRegistry.getRegistry(_dfsRegistryHost, _dfsRegistryPort);
            _dfsService = (DfsService) _dfsRegistry.lookup("DfsService");                                               
        }
        catch (RemoteException e) {
            System.out.println("Remote Exception:");
            System.out.println(e.getMessage());
            System.exit(0);
        }
        catch (NotBoundException e) {
            System.out.println("Registry not bound:");
            System.out.println(e.getMessage());
            System.exit(0);
        }
        
        _dnRegistries = new HashMap<String, Registry>();
        _dnServices = new  HashMap<String, Node>();
        for(int i=0; i<_dnRegistryHosts.size(); i++) {
            try {            
                _dnRegistries.put(_dnRegistryHosts.get(i), 
                        LocateRegistry.getRegistry(_dnRegistryHosts.get(i), _dnRegistryPort));
                _dnServices.put(_dnRegistryHosts.get(i), 
                        (Node) _dnRegistries.get(_dnRegistryHosts.get(i)).lookup("DataNode"));
            }             
            catch (RemoteException e) {
                //set the datanode registry and service to null for this node
                //can't print the following message to the user, because the user may not be in charge of the cluster.
                //System.out.println("Remote Exception. Please note that datanode "+_dnRegistryHosts.get(i)+" is not accessible.");
                _dnRegistries.put(_dnRegistryHosts.get(i), null);
                _dnServices.put(_dnRegistryHosts.get(i), null);
            }
            catch (NotBoundException e) {
                //set the datanode registry and service to null for this node
                //System.out.println("Registry not bound. Datanode "+_dnRegistryHosts.get(i)+" not accessible.");                
                _dnRegistries.put(_dnRegistryHosts.get(i), null);
                _dnServices.put(_dnRegistryHosts.get(i), null);
            }
        }
        
        try {
            _hostName = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            System.out.println("Unknown host exception:");
            System.out.println(e.getMessage());
            System.exit(0);
        }
        
	}
	
	@Override
	public synchronized boolean checkFileExists(String dfsPath) {
	    boolean answer = false;
	    try {
            if(!_dfsService.checkPathValidity(dfsPath, _hostName, true)) {
                System.out.println("Invalid file path.");
                return false;
            }
        }
        catch (RemoteException e1) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);            
        }
	    try {	        
            answer = _dfsService.checkFileExists(dfsPath, _hostName);
        }
        catch (RemoteException e) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);
        }
	    return answer;
	}
	
	@Override
	public void addFileToDfs(String inPath, String dfsPath, InputSplit inputSplit, boolean overwrite) {		    
	    //check if input file exists
	    if(!new File(inPath).exists()) {
	        System.out.println("ERROR: Input file does not exist/incorrect path.");
	        return;
	    }
	    //check if the DFS path is valid
	    try {
            if(!_dfsService.checkPathValidity(dfsPath, _hostName, true)) {
                //invalid path
                System.out.println("Invalid DFS path.");
                return;
            }
        }
        catch (RemoteException e3) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);
        }
	    
	    //number of 64MB blocks needed    
	    int numBlocks = (int)Math.ceil((double)(new File(inPath).length())/_blockSize);
	    
	    Map<String, List<String>> blocks = new HashMap<String, List<String>>();
	    try {        
            //get the datanode to block map from the DFS
            blocks = _dfsService.addInputFileToDfs(dfsPath, _hostName, numBlocks, overwrite);
            System.out.println(blocks.hashCode());
        }	    
        catch (RemoteException e) {
            System.out.print("Remote Exception: ");
            System.out.println(e.getMessage());
            System.exit(0);
        }        
	    System.out.println("this happened 1");
	    //create tmp dir where file blocks will be stored on client side
	    File tempDirOnUserSystem = new File("tmp");
	    if(tempDirOnUserSystem.exists()) {
            File[] files = tempDirOnUserSystem.listFiles();
            if(files!=null) {
                for(File f: files) {
                    f.delete();	                    
                }
            }
            tempDirOnUserSystem.delete();        
	    }
	    tempDirOnUserSystem.mkdir();
	    
	    int startPos = 0;
	    //sort the block names received from DFS
	    for(Entry<String, List<String>> entry: blocks.entrySet()) {
	        System.out.print(entry.getKey()+": ");
	        for(String node: entry.getValue()) {
	            System.out.print(node+", ");
	        }
	        System.out.println();
	    }
	    Map<String, List<String>> sortedBlockMap = new TreeMap<String, List<String>>(blocks);
	    for(Entry<String, List<String>> entry: sortedBlockMap.entrySet()) {
	        if(startPos == -1) {
	            //shouldn't happen because the for loop will exit before this
	            break;
	        }
            //create new file for each block	        
            startPos = createBlock(inPath, tempDirOnUserSystem.getPath()+"/"+entry.getKey(), startPos, inputSplit);
            if(startPos == Integer.MIN_VALUE) {
                //error
                System.out.println("Program exiting..");
                System.exit(0);
            }
            System.out.println("Got blocks");
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
            //send blocks to datanodes
            for(String datanode: entry.getValue()) {
                Node node = _dnServices.get(datanode);
                if(node == null) {
                    //shouldn't happen
                    System.out.println("Please add the file again to DFS by setting OVERWRITE=TRUE.");
                    //System.exit(0);
                } else {
                    try {                        
                        String remoteFilePath = _localBaseDir + entry.getKey();
                        //create file on datanode
                        node.createFile(remoteFilePath);
                        //send bytes to datanode to write
                        RandomAccessFile file = new RandomAccessFile(tempDirOnUserSystem.getPath()+"/"+entry.getKey(), "r");
                        byte[] buffer = new byte[1000];
                        int start = 0;
                        while(file.read(buffer) != -1) {
                            node.writeToFile(remoteFilePath, buffer, start);                            
                            buffer = new byte[1000];
                            start += 1000;
                        }
                        file.close();
                        //confirm block receipt
                        _dfsService.confirmBlockAndNodeNameReceipt(entry.getKey()+"--"+datanode);                                                
                    }
                    catch (RemoteException e) {
                        //TODO: ask DFS for another node to put this block in
                        System.out.println("Seems like the DFS service or a datanode went down."
                                + "Cheching to see which one it is.");                        
                        try {
                            System.out.println("I think a datanode is down: "+datanode);
                            _dfsService.reportFailedNode(datanode, dfsPath, _hostName, true);
                            System.out.println("I've reported the failure to the Namenode. "
                                    + "However, I will have to ask you to add the file again to DFS. "
                                    + "Sorry about that.");
                            
                        }
                        catch (RemoteException e1) {
                            System.out.println("Mayday! Mayday! The Namenode, in fact, is the one that's down. "
                                    + "Cannot continue. This is the worst disaster in the history of this system."
                                    + "Need time to grieve. Goodbye!");
                        } finally {
                            System.exit(0);
                        }
                    }
                    catch (FileNotFoundException e) {
                        System.out.println("File not found exception:");
                        e.printStackTrace();
                    }
                    catch (IOException e) {
                        System.out.println("IO Exception:");
                        e.printStackTrace();
                    }
                }
            }
            //delete the block from user's local disk
            new File(tempDirOnUserSystem.getPath()+"/"+entry.getKey()).delete();
        }	 	    
	    //delete temp dir on user file system if empty
	    if(tempDirOnUserSystem.listFiles().length == 0) {
	        tempDirOnUserSystem.delete();
	    }
	}
	
	@Override
    public void getFileFromDfs(String dfsPath, String outputPath)
    {
	  //check if the DFS path is valid
        try {
            if(!_dfsService.checkPathValidity(dfsPath, _hostName, true)) {
                //invalid path
                System.out.println("Invalid DFS path.");
                return;
            }
        }
        catch (RemoteException e3) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);
        }
        if(!checkFileExists(dfsPath)) {
            System.out.println("File does not exist on DFS.");
            return;
        }
        
	    Map<String, List<String>> blocks = null;
        try {
            blocks = _dfsService.getFileFromDfs(dfsPath, _hostName);            
        }
        catch (RemoteException e) {
            System.out.println("Remote Exception (Couldn't get file):");
            System.out.println(e.getMessage());
            return;
        }
        
        for(Entry<String, List<String>> entry: blocks.entrySet()) {
            //create new file on the user path
            String localFileName = outputPath+entry.getKey();
            File file = new File(localFileName);
            File parent = file.getParentFile();
            if(!parent.exists() && !parent.mkdirs()){
                System.out.println("Couldn't create directory "+parent+" on datanode.");
                return;
            }
            try {
                if(file.exists()) {
                    //TODO: delete?
                    file.delete();
                }
                file.createNewFile();
            }
            catch (IOException e) {
                System.out.println("IO Exception:");
                System.out.println(e.getMessage());
                return;
            }        
            
            boolean transferred = false;
            for(String datanode: entry.getValue()) {
                Node node = _dnServices.get(datanode);
                if(node == null) {
                    //shouldn't happen because DFS service sends only those nodes that are
                    //currently available.
                    continue;
                } else {
                    try {
                        String remoteFilePath = _localBaseDir + entry.getKey();
                        int start = 0;
                        RandomAccessFile raf = new RandomAccessFile(localFileName, "rw");
                        byte[] bytes = new byte[1000];
                        while((bytes = node.getFile(remoteFilePath, start)) != null) {
                            raf.seek(start);
                            raf.writeBytes(new String(bytes));
                            bytes = new byte[1000];
                            start += 1000;
                        }
                        raf.close();
                        transferred = true;
                        break;
                    }
                    catch (RemoteException e) {
                        //continue with another block in the array
                        //notify the DFS that this node failed
                        try {
                            System.out.println("I think a datanode is down: "+datanode);
                            _dfsService.reportFailedNode(datanode, dfsPath, _hostName, false);
                            System.out.println("I've reported the failure to the Namenode. "
                                    + "However, I will try downloading again from another node (if there is one).");
                            
                        }
                        catch (RemoteException e1) {
                            System.out.println("The DFS/Namenode also seems to be down. However, I will try downloading the file"
                                    + "from another datanode if available.");
                        }
                    }
                    catch (FileNotFoundException e) {
                        //shouldn't happen                     
                        System.out.println("Local file not found.");
                        System.out.println(e.getMessage());
                        //can't continue because there is no file to write to
                        break;
                    }
                    catch (IOException e) {
                        System.out.println("IO Exception:");
                        System.out.println(e.getMessage());
                        System.out.println("Will try downloading again from another node (if there is one).");
                    }
                }                
            }    
            if(!transferred) {
                //the block was not transferred to local file system because ALL the nodes
                //that had a copy of it are down                
                System.out.println("ERROR: Could not download the block: "+entry.getKey());
                System.out.println("All the nodes with that block are down.");
            } else {
                System.out.println("Succesfully downloaded block: "+entry.getKey());
            }
            
        }
        
        
    }
	
	/**
	 * Prints the current DFS file structure.
	 */
	@Override
    public String printDFSStructure()
    {        
        try {
            return _dfsService.printDfsStructure();
        }
        catch (RemoteException e) {
            System.out.println("Remote exception:");
            System.out.println(e.getMessage());
        }
        return null;
    }
	
	/**
	 * Deletes file from DFS.
	 * @param dfsPath The path of the file on DFS.
	 */
	@Override
	public void deleteFileFromDfs(String dfsPath) {
	  //check if the DFS path is valid
        try {
            if(!_dfsService.checkPathValidity(dfsPath, _hostName, true)) {
                //invalid path
                System.out.println("Invalid DFS path.");
                return;
            }
        }
        catch (RemoteException e3) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);
        }
        if(!checkFileExists(dfsPath)) {
            System.out.println("File does not exist on DFS.");
            return;
        }
        
	    try {
            _dfsService.deleteFileFromDfs(dfsPath, _hostName);
        }
        catch (RemoteException e) {
            System.out.println("Remote exception:");
            System.out.println(e.getMessage());
        }
	}
	
	@Override
    public void getDirFromDfs(String dfsPath, String outputPath)
    {
	    try {
            if(!_dfsService.checkPathValidity(dfsPath, _hostName, false)) {
                //invalid path
                System.out.println("Invalid DFS path.");
                return;
            }
        }
        catch (RemoteException e3) {
            System.out.println("Could not connect to DFS service.");
            System.exit(0);
        }
	    Map<String, String> map = null;
	    try {
            map = _dfsService.getDirFromDfs(dfsPath, _hostName);
        }
        catch (RemoteException e) {
            System.out.println("Could not connect to DFS registry.");
            System.exit(0);
        }
	    
	    for(Entry<String, String> entry: map.entrySet()) {
	        getFileFromDfs(entry.getValue()+entry.getKey(), "testOP2/"+entry.getValue());	        
	    }	    
    }
	
	@Override
	public void startMapReduce(String jarPath, String mapperClassName) {
	    //check if input jar file exists
        if(!new File(jarPath).exists()) {
            System.out.println("ERROR: Jar file does not exist/incorrect path.");
            return;
        } else if(!jarPath.endsWith(".jar")) {
            System.out.println("ERROR: Need a Jar file.");
            return;
        }
	    for(Entry<String, Node> service: _dnServices.entrySet()) {
	        Node node = service.getValue();
	        if(node == null) {
	            continue;
	        }
	        //send jar to all active nodes	        
	        String[] jarSplitPath = jarPath.split("/");
	        String jarFileName = jarSplitPath[jarSplitPath.length-1];
	        try {                	            
                //create file on datanode
	            String remoteJarPath = _localBaseDir+_hostName+"/"+jarFileName;  // e.g. /tmp/localhost--test.jar
                node.createFile(remoteJarPath);
                //send bytes to datanode to write
                RandomAccessFile file = new RandomAccessFile(jarPath, "r");
                byte[] buffer = new byte[1000];
                int start = 0;
                int count = 0;
                while((count = file.read(buffer)) > 0) {                    
                    node.sendJarFile(remoteJarPath, buffer, start, count);                            
                    buffer = new byte[1000];
                    start += 1000;
                }
                file.close();   
                node.testRunJar(remoteJarPath, mapperClassName);
            }
            catch (RemoteException e) {
                System.out.println("Remote Exception:");
                System.out.println(e.getMessage());                                                        
            }
            catch (FileNotFoundException e) {
                System.out.println("File not found exception:");
                e.printStackTrace();
            }
            catch (IOException e) {
                System.out.println("IO Exception:");
                e.printStackTrace();
            }
	    }
	    
	}
	
	/**
	 * 
	 * @param inFilePath
	 * @param outFilePath
	 * @param startPos
	 * @param inputSplit
	 * @return The position from the the next call to the same file should start reading. Returns
	 *         Integer.MIN_VALUE for error, and -1 for end of file.
	 */
	private int createBlock(String inFilePath, String outFilePath, int startPos, InputSplit inputSplit) {
	    //create output block file
	    File outFile = new File(outFilePath);
	    BufferedWriter bufFileWriter = null;
	    try {
            if(!outFile.createNewFile()) {
                System.out.println("Couldn't create block.");
                return Integer.MIN_VALUE;
            }
            bufFileWriter = new BufferedWriter(new FileWriter(outFile));
        }
        catch (IOException e1) {
            System.out.println("IO exception:");
            System.out.println(e1.getMessage());
            return Integer.MIN_VALUE;
        }
	    
	    int lastPos = startPos;
	    String splitParam = inputSplit.getSplitParam();
	    try {
	        @SuppressWarnings("resource")
            RandomAccessFile file = new RandomAccessFile(inFilePath, "r");
	        file.seek(startPos);
	        
	        if(splitParam.equals("c")) {
	            //split according to character delimiter
	            char delim = inputSplit.getDelimiter();
	            String record = "";
	            int fileSize = 0;  //keep track of how many characters have been written to the block
	            int nextChar = 0;
	            while((nextChar = file.read()) != -1) {
	                //read character by character till the delim is reached
	                char c = (char)nextChar;
	                if(c==delim) {
	                    //check if the length of the file exceeds the block size
	                    if(fileSize >= _blockSize) {
	                        //can't add this record	                        
	                        bufFileWriter.close();	
	                        file.close();
	                        //return the start of the record that was last read
	                        //so that it is read again for the next block
	                        return startPos;
	                    } else {
	                        //add the record
	                        bufFileWriter.append(record+_recordDelimiter);
	                        bufFileWriter.flush();
	                        lastPos += 1;
	                        fileSize += _recordDelimiter.length();
	                        startPos = lastPos;	              
	                        record = "";
	                    }
	                } else {
    	                record+=c;	                
    	                lastPos++;
    	                fileSize++;
	                }
	            }	            
	        } else if(splitParam.equals("b")) {
                //split according to number of bytes per record
                int fileSize = 0;  //keep track of how many bytes have been written to the block
                int byteSize = inputSplit.getBytes();
                byte[] byteInput = new byte[byteSize];
                while(file.read(byteInput) != -1) {
                    //read character by character till the delim is reached
                    if(fileSize >= _blockSize) {
                        //can't add this record                         
                        bufFileWriter.close();  
                        file.close();
                        //return the start of the record that was last read
                        //so that it is read again for the next block
                        return startPos;
                    } else {
                        //add the record
                        bufFileWriter.append(new String(byteInput));//+_recordDelimiter);
                        bufFileWriter.flush();
                        startPos += byteSize;
                        fileSize += byteSize;// + _recordDelimiter.length(); 
                        Arrays.fill(byteInput, (byte)0);
                    }
                }        
            }	        
        }
        catch (FileNotFoundException e) {
            System.out.println("File not found:");
            System.out.println(e.getMessage());
            System.out.println("System exiting.");
            System.exit(0);
        }	    
        catch (IOException e) {
            System.out.println("File IO exception:");
            System.out.println(e.getMessage());
            System.out.println("System exiting.");
            System.exit(0);
        }
	    //reached end of file
	    return -1;	    
	}

}
