package dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import mapred.JobTracker;

public class DfsRegGenerator
{
    public static void main(String[] args) throws FileNotFoundException, IOException {
        if (System.getSecurityManager() == null) {
            System.setProperty("java.security.policy", "server.policy"); 
            System.setSecurityManager(new SecurityManager());                       
        }
        final DfsService_Impl service = new DfsService_Impl();                
        final String name = "DfsService";        
        final DfsService stub =
                (DfsService) UnicastRemoteObject.exportObject(service, 0);
        final Object monitor = new Object();

        new Thread(new Runnable() {
            public void run() {
                try {                  
                    System.out.println(service._registryPort);
                    Registry registry = LocateRegistry.createRegistry(service._registryPort);
                    registry.rebind(name, stub);
                    //JobTracker jt = new JobTracker();
                    //jt.exec();
                    synchronized (monitor) {
                        monitor.wait();                        
                    }
                    UnicastRemoteObject.unexportObject(service, true); 
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }                
                System.out.println("Exiting DFS Service. Registry will no longer be available.");                
            }
        }, "DfsRegGeneratorThread").start();        
        System.out.println("Press enter to exit the DFS Service.");
        System.in.read();
        synchronized (monitor) {        
            monitor.notify();                 
        }        
    }             
}
