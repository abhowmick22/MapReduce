package datanode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import mapred.TaskTracker;
import dfs.DfsService;

public class DataNodeRegGenerator
{
    public static void main(String[] args) throws FileNotFoundException, IOException {
                  
        if (System.getSecurityManager() == null) {
            System.setProperty("java.security.policy", "server.policy"); 
            System.setSecurityManager(new SecurityManager());                       
        }
        final Node_Impl service = new Node_Impl();
        final String name = "DataNode";
        final String dfsServiceName = "DfsService";
        
        final Node stub =
                (Node) UnicastRemoteObject.exportObject(service, 0);
        final Object monitor = new Object();

        new Thread(new Runnable() {
            public void run() {
                try {                  
                    System.out.println(service._registryPort);
                    Registry registry = LocateRegistry.createRegistry(service._registryPort);
                    registry.rebind(name, stub); 
                    //notify the DFS that this datanode is up and running
                    FileReader fr = new FileReader("ConfigFile");   //TODO: change the name
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
                        if(key.equals("DFS-RegistryPort")) {
                            dfsRegistryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                        } else if(key.equals("DFS-RegistryHost")) {
                            dfsRegistryHost = keyValue[1].replaceAll("\\s", "");
                        }          
                    }
                    if(dfsRegistryPort == -1 || dfsRegistryHost.equals("")) {
                        System.out.println("Config file not set up correctly. DFS registry host/port not found.");
                        br.close();
                        System.exit(0);
                    }
                    br.close();
                    List<String> thisNode = new ArrayList<String>();
                    thisNode.add(InetAddress.getLocalHost().getHostName());
                    ((DfsService) LocateRegistry.getRegistry(dfsRegistryHost, dfsRegistryPort)
                            .lookup(dfsServiceName)).updateActiveNodes(thisNode, false);
                    TaskTracker tt = new TaskTracker();
                    tt.exec();
                    synchronized (monitor) {
                        monitor.wait();                        
                    }
                    UnicastRemoteObject.unexportObject(service, true);
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (FileNotFoundException e) {
                    System.out.println("Config file not found.");
                    e.printStackTrace();
                    System.exit(0);
                }
                catch (IOException e) {
                    System.out.println("Config file IO problem.");
                    e.printStackTrace();
                    System.exit(0);
                }
                catch (NotBoundException e) {                    
                    System.out.println("DFS Registry not bound.");
                    e.printStackTrace();
                    System.exit(0);
                }
                System.out.println("Exiting Datanode Service. Registry will no longer be available.");
            }
        }, "DataNodeRMIThread").start();
        System.out.println("Press enter to exit the datanode service.");
        System.in.read();
        synchronized (monitor) {
            monitor.notify();            
        }
    }
        
     
}
