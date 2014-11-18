package datanode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNodeRegGenerator
{
    public static void main(String[] args) throws FileNotFoundException, IOException {
        final Node_Impl service = new Node_Impl();              
        if (System.getSecurityManager() == null) {
            System.setProperty("java.security.policy", "server.policy"); 
            System.setSecurityManager(new SecurityManager());                       
        }
        final String name = "DataNode";
        
        //read config file and set corresponding values; also initialize the root directory of DFS        
        final Node stub =
                (Node) UnicastRemoteObject.exportObject(service, 0);
        final Object monitor = new Object();

        new Thread(new Runnable() {
            public void run() {
                try {                  
                    System.out.println(service._registryPort);
                    Registry registry = LocateRegistry.createRegistry(service._registryPort);
                    registry.rebind(name, stub);                    
                    synchronized (monitor) {
                        monitor.wait();                        
                    }
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("RMI Registry Thread finished.");
            }
        }, "RMI Registry Thread").start();
        System.out.println("Press enter to exit...");
        System.in.read();
        synchronized (monitor) {
            monitor.notify();            
        }
    }
        
     
}
