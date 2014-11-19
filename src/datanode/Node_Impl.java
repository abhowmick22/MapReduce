package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import datanode.exceptions.BlockNotFoundException;
import dfs.DfsService;


public class Node_Impl implements Node
{
    public int _registryPort;          //registry port 
    private String _nodeName;           //name of this node
    private DfsService _dfsService;     //handle for DFS service
        
    public Node_Impl() {
        
        if (System.getSecurityManager() == null) {
            System.setProperty("java.security.policy", "client.policy");
            System.setSecurityManager(new SecurityManager());
        }
        
        FileReader fr = null;
        try {
            //TODO: perform input checks
            fr = new FileReader("tempDfsConfigFile");   //TODO: change the name
            BufferedReader br = new BufferedReader(fr);            
            String line = "";
            String dfsRegistryHost = "";
            int dfsRegistryPort = -1;
            while((line=br.readLine())!=null) {  
                
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if (key.equals("DN-RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));                                            
                } else if(key.equals("DFS-RegistryHost")) {
                    dfsRegistryHost = keyValue[1].replaceAll("\\s", "");
                } else if(key.equals("DFS-RegistryPort")) {
                    dfsRegistryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                }
            }          
            br.close();            
            _nodeName = InetAddress.getLocalHost().getHostName();
            if(dfsRegistryPort == -1 || dfsRegistryHost.equals("")) {
                System.out.println("DFS Registry port/host not found. Program exiting.");
                System.exit(0);
            } 
            //get the DFS service handle
            try {
                _dfsService = (DfsService) LocateRegistry.getRegistry(dfsRegistryHost, dfsRegistryPort).lookup("DfsService");
            }
            catch (NotBoundException e) {
                System.out.println("DFS Registry not bound. Program exiting.");
                System.exit(0);
            }                                               
           
            
        }
        catch (FileNotFoundException e) {
            System.out.println("EXCEPTION: Config file not found.");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("EXCEPTION: Config file IO exception.");
            System.exit(0);
        }   
        
    }
    
    @Override
    public synchronized boolean createFile(String path) throws RemoteException{
        File file = new File(path);
        File parent = file.getParentFile();
        if(!parent.exists() && !parent.mkdirs()){
            throw new RemoteException("Couldn't create directory "+parent+" on datanode.");
        }
        try {
            if(file.exists()) {
                //TODO: delete?
                //if the file block already existed, it will be deleted because we assume that
                //every datanode maintains only 1 copy of a file node.
                file.delete();
            }
            file.createNewFile();
        }
        catch (IOException e) {
            throw new RemoteException("File not created: "+path);
        }        
        return true;
    }
    
    @Override
    public synchronized boolean writeToFile(String path, byte[] bytes, int start) throws RemoteException{        
        try {            
            RandomAccessFile raf = new RandomAccessFile(path, "rw");
            raf.seek(start);
            raf.writeBytes(new String(bytes));
            raf.close();
        }
        catch (IOException e) {
            throw new RemoteException("Problem writing to file: "+path);            
        }
        return true;
    }   
    
    @Override
    public byte[] getFile(String path, int start) throws RemoteException {
        try {            
            RandomAccessFile raf = new RandomAccessFile(path, "r");
            raf.seek(start);
            byte[] buffer = new byte[1000];
            if(raf.read(buffer) != -1) {
                raf.close();
                return buffer;
            }
            raf.close();
            return null;
            
        }
        catch (IOException e) {
            throw new RemoteException("Problem writing to file: "+path);            
        }
    }

    @Override
    public boolean transferBlockTo(Node destNode, String path)
        throws RemoteException
    {
        //get the destination node's service and transfer block to it       
        try {            
            //datanode registry port is the same for all datanodes
            destNode.createFile(path);
            RandomAccessFile file = new RandomAccessFile(path, "r");    //same path of a file block on both datanodes
            byte[] buffer = new byte[1000];
            int start = 0;
            while(file.read(buffer) != -1) {
                destNode.writeToFile(path, buffer, start);        //same path of a file block on both datanodes                    
                buffer = new byte[1000];
                start += 1000;
            }
            file.close();
            //confirm block receipt
            String[] pathArray = path.split("/");  
            String blockname = pathArray[pathArray.length-1];   //because the blockname is the last part of the path
            
            //confirm block receipt to DFS service
            _dfsService.confirmBlockAndNodeNameReceipt(blockname+"--"+destNode.getNodeName());
        }             
        catch (RemoteException e) {
            throw e;
        }
        catch (IOException e) {
            throw new RemoteException("IOException on source datanode: " + _nodeName);
        }
        return true;
    }
    
    @Override
    public String getNodeName() {
        return _nodeName;
    }

    @Override
    public void deleteFile(String path)
        throws RemoteException
    {
        System.out.println(path);
        if(!new File(path).exists()) {
            throw new BlockNotFoundException();
        }
        new File(path).delete();            
    }
}
