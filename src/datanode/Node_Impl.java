package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

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
        if(parent != null && !parent.exists() && !parent.mkdirs()){
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
            return true;
        }             
        catch (RemoteException e) {
            throw e;
        }
        catch (IOException e) {
            throw new RemoteException("IOException on source datanode: " + _nodeName);
        }        
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
    
    //TODO: shift to mapper/reducer
    @Override   
    public void testRunJar(String jarPath, String mapperClassName) {
        System.out.println(jarPath);
        System.out.println(mapperClassName);
        File file = new File(jarPath);
        mapperClassName = "bin.JarTest";
        java.net.URL[] url = new java.net.URL[1];
        try {
            url[0] = file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        java.net.URLClassLoader urlClassLoader = new java.net.URLClassLoader(url, this.getClass().getClassLoader());
        Class mapperClass = null;
        System.out.println(urlClassLoader);
        try {
            mapperClass = Class.forName(mapperClassName, true, urlClassLoader);
            java.lang.reflect.Method method = mapperClass.getDeclaredMethod ("map");
            Object instance = mapperClass.newInstance();
            method.invoke(instance);
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
        
    }

    @Override
    public void sendJarFile(String jarPath, byte[] bytes, int start, int count)
        throws RemoteException
    {        
        try {           
            
            RandomAccessFile raf = new RandomAccessFile(jarPath, "rw");
            raf.seek(start);
            raf.write(bytes, 0, count);
            raf.close();
        }
        catch (IOException e) {
            throw new RemoteException("Problem writing to file: "+jarPath);            
        }
    }   
}
