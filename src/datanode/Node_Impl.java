package datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.rmi.RemoteException;


public class Node_Impl implements Node
{
    int _registryPort;          //registry port 
    
    
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
                }              
            }          
            br.close();
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
}
