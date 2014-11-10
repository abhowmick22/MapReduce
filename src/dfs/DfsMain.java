/**
 * The main class for the distributed file system.
 */
package dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import dfs.dontknowifneeded.DfsClientApiMsg;

public class DfsMain
{
    final String dfsPathIndentifier = "/dfs/";    //every path on dfs should start with this
    
    private Map<String, Integer> dataNodeLoad;     //maintains a map between datanode and load on it in terms of number of blocks stored on it
    private int repFactor;                         //replication factor
    private String[] dataNodeNames;                //list of datanode names
    private int dataNodesNum;                      //total number of datanodes
    private DfsStruct rootStruct;                  //the root of the trie which represents the directory structure
    private int nameNodePort;                      //port that namenode listens to
        
    
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
                switch(keyValue[0].replaceAll("\\s", "")) {
                    case "TotalDataNodes": {
                        this.dataNodesNum = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        break;
                    }
                    case "DataNodes": {
                        this.dataNodeNames = keyValue[1].split(",");
                        //remove whitespaces
                        for(int i=0; i<this.dataNodeNames.length; i++) {
                            this.dataNodeNames[i] = this.dataNodeNames[i].replaceAll("\\s", "");
                        }
                        break;
                    }
                    case "ReplicationFactor": {
                        this.repFactor = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        break;
                    }
                    case "NameNodePort": {
                        this.nameNodePort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
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
            this.rootStruct = new DfsStruct("dfs", "/dfs/");
            
        }
        catch (FileNotFoundException e) {
            System.out.println("EXCEPTION: Config file not found.");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Config file IO exception.");
            System.exit(0);
        }
        
        this.dataNodeLoad = new HashMap<String, Integer>();
        for(String dataNode: this.dataNodeNames) {
            System.out.println(dataNode);
            this.dataNodeLoad.put(dataNode, 0);
        }        
        //TODO: Use this for sending list of datanodes to distribute the file on
//        Map<String, Integer> map = new TreeMap<String, Integer>(new LoadComparator(dataNodeLoad));
//        map.putAll(dataNodeLoad);
//        
//        for(Entry<String, Integer>entry: map.entrySet()) {
//            System.out.println(entry.getKey()+"-"+entry.getValue());
//        }
    }
    
    public void handleRequests(String command, ObjectInputStream inStream, ObjectOutputStream outStream) {
        switch(command) {
            case "NewFile": {
                //message received from client api, so expect a DfsClientApiMessage
                //tasks: 1. create corresponding folders and file in DFS
                //       2. send back the datanode names where to replicate, along with local path
                DfsClientApiMsg clientApiMsg;
                try {
                    clientApiMsg = (DfsClientApiMsg) inStream.readObject();
                }
                catch (ClassNotFoundException | IOException e) {
                    System.out.println("EXCEPTION: Problem reading on server socket on namenode. File not created on DFS.");
                    return;
                }
                String[] inPath = clientApiMsg.getInPath().split("/");
                String[] outPath = clientApiMsg.getOutPath().split("/");
                //check validity of inPath and outPath
                
                break;
            }
            default: {
                break;
            }
        }
    }
    
    /**
     * Returns the data node with minimum load (in terms of disk space)
     * @return Data node with minimum load.
     */
    private String getMinLoad() {
        int min = Integer.MAX_VALUE;
        String minNode = null;
        for(Entry<String, Integer> entry : this.dataNodeLoad.entrySet()) {
            if(entry.getValue() < min) {
                min = entry.getValue();
                minNode = entry.getKey();
            }
        }
        
        return minNode;
    }
    
    private class LoadComparator implements Comparator<String> {
        private Map<String, Integer> map;        
        public LoadComparator(Map<String, Integer> map) {
            this.map = map;
        }        
        public int compare(String key1, String key2)
        {
            return map.get(key1) >= map.get(key2) ? 1 : -1;            
        }        
    }
    
    public static void main(String[] args) {   
        DfsMain dfsMain = new DfsMain();
        //read config file and set corresponding values; also initialize the root directory of DFS
        dfsMain.dfsInit();
        //TODO: spawn JobTracker
        //now listen to requests from ClientAPI's and datanodes
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(dfsMain.nameNodePort);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Problem creating server socket on namenode. Program exiting.");
            System.exit(0);
        }
        while(true) {
            Socket newConnection = null;
            try {
                newConnection = serverSocket.accept();
            }
            catch (IOException e) {
                System.out.println("EXCEPTION: Problem accepting connection on server socket on namenode.");
                continue;
            }
            //process request in this thread itself
            //TODO: START FROM HERE -- request types could be: put file in dfs + create folder + send nodenames to store data, 
            //inpath/outpath validity, create folders for outpath, check path name validity (part 
            //of previous three), notice from JobTracker about node going down->replicate
            ObjectInputStream inStream;
            ObjectOutputStream outStream;
            String command;
            try {
                inStream = new ObjectInputStream(newConnection.getInputStream());
                outStream = new ObjectOutputStream(newConnection.getOutputStream());
                command = (String) inStream.readObject();
            }
            catch (IOException e) {
                System.out.println("EXCEPTION: Problem reading from input stream on network.");
                continue;
            }
            catch (ClassNotFoundException e) {
                System.out.println("EXCEPTION: Problem reading correct input format on network.");
                continue;
            }            
            dfsMain.handleRequests(command, inStream, outStream);
        }
        
    }
    
}
