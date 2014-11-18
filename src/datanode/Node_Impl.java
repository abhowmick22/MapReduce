package datanode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
            file.createNewFile();
        }
        catch (IOException e) {
            throw new RemoteException("File not created: "+path);
        }        
        return true;
    }
    
    @Override
    public synchronized boolean writeToFile(String path, byte[] bytes) throws RemoteException{        
        try {
            (new BufferedWriter(new FileWriter(path))).append(bytes.toString());
        }
        catch (IOException e) {
            throw new RemoteException("Problem writing to file: "+path);            
        }
        return true;
    }    
}
