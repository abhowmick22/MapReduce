package clientapi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import dfs.DfsService;
import dfs.InputSplit;

public class ClientApi_Impl implements ClientApi {

    final int _blockSize = 2*1000*1000;        //TODO: put this somewhere else, also change size to 64MB. this is test size.
    final String _recordDelimiter = "\n";         //TODO: put this somewhere else, and use this to read records
    
	private int _registryPort;			//registry port 
	private String _registryHost;       //registry host
	private Registry _dfsRegistry;      //handle for DFS registry
	private DfsService _dfsService;     //handle for DFS service   
	
	public ClientApi_Impl() {
		
		if (System.getSecurityManager() == null) {
		    System.setProperty("java.security.policy", "client.policy");
            System.setSecurityManager(new SecurityManager());
        }
		
		FileReader fr = null;
        try {
            fr = new FileReader("tempDfsConfigFile");   //TODO: change the name
            BufferedReader br = new BufferedReader(fr);
            _registryPort = -1;
            _registryHost = "";            
            String line;
            while((line=br.readLine())!=null) {  
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if(key.equals("RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                } else if(key.equals("RegistryHost")) {
                    _registryHost = keyValue[1].replaceAll("\\s", "");
                }                
            }
            if(_registryPort == -1 || _registryHost.equals("")) {
                System.out.println("Registry port/host not found. Program exiting..");
                System.exit(0);
            } 
            
        } catch (Exception e) {
            System.err.println("DfsService exception:");
            e.printStackTrace();
        }
        
        //get DFS Service handle         
        try {
            String name = "DfsService";
            _dfsRegistry = LocateRegistry.getRegistry(_registryHost, _registryPort);
            _dfsService = (DfsService) _dfsRegistry.lookup(name);            
        }
        catch (RemoteException e) {
            System.out.println("Remote Exception:");
            e.printStackTrace();
            System.exit(0);
        }
        catch (NotBoundException e) {
            System.out.println("Registry not bound:");
            e.printStackTrace();
            System.exit(0);
        }
        
	}
		
	public void addFileToDFS(String inPath, String dfsPath, InputSplit inputSplit) {		    
	    //check if input file exists
	    if(!new File(inPath).exists()) {
	        System.out.println("ERROR: Input file does not exist/incorrect path.");
	        return;
	    }
	    //number of 64MB blocks needed    
	    int numBlocks = (int)Math.ceil((double)(new File(inPath).length())/_blockSize);
	    
	    Map<String, List<String>> blocks = null;
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            //get the datanode to block map from the DFS
            blocks = _dfsService.addFileToDfs(dfsPath, hostname, numBlocks);
            for(Entry<String, List<String>> entry: blocks.entrySet()) {
                System.out.print(entry.getKey()+": ");
                List<String> list = entry.getValue();
                for(String value: list) {
                    System.out.print(value+", ");
                }
                System.out.println();
            }             
        }
        catch (RemoteException e) {
            System.out.println("Remote Exception:");
            e.printStackTrace();
        }
        catch (UnknownHostException e) {
            System.out.println("Unknown host exception:");
            e.printStackTrace();
            System.exit(0);
        }
        
	    /*
	    String[] fileBlockNames = new String[numBlocks];
	    for(int i = 0; i<numBlocks; i++)
	        fileBlockNames[i] = "temp-"+i;
	    
	    //create tmp dir where file blocks will be stored on client side
	    File tempDir = new File("tmp");
	    if(tempDir.exists()) {
            File[] files = tempDir.listFiles();
            if(files!=null) {
                for(File f: files) {
                    f.delete();	                    
                }
            }
            tempDir.delete();        
	    }
	    tempDir.mkdir();
	    
	    int startPos = 0;
	    for(int i=0; i<numBlocks; i++) {
	        if(startPos == -1) {
	            //shouldn't happen because the for loop will exit before this
	            break;
	        }
            //create new file for each block	        
            startPos = createBlock(inPath, tempDir.getPath()+"/"+fileBlockNames[i], startPos, inputSplit);
            if(startPos == Integer.MIN_VALUE) {
                //error
                System.out.println("Program exiting..");
                System.exit(0);
            } 
        }	    
	    */
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
            e.printStackTrace();
        }
        return null;
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
            e1.printStackTrace();
            return Integer.MIN_VALUE;
        }
	    
	    int lastPos = startPos;
	    String splitParam = inputSplit.getSplitParam();
	    try {
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
                        bufFileWriter.append(new String(byteInput)+_recordDelimiter);
                        bufFileWriter.flush();
                        startPos += byteSize;
                        fileSize += byteSize + _recordDelimiter.length(); 
                        Arrays.fill(byteInput, (byte)0);
                    }
                }        
            }	        
        }
        catch (FileNotFoundException e) {
            System.out.println("File not found:");
            e.printStackTrace();
            System.out.println("System exiting.");
            System.exit(0);
        }	    
        catch (IOException e) {
            System.out.println("File IO exception:");
            e.printStackTrace();
            System.out.println("System exiting.");
            System.exit(0);
        }
	    //reached end of file
	    return -1;	    
	}

}
