package clientapi;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import dfs.DfsService;

public class ClientApi_Impl implements ClientApi {

	private int _registryPort;			//port 
	
	public ClientApi_Impl() {
		
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        try {
            String name = "Compute";
            Registry registry = LocateRegistry.getRegistry();
            DfsService dfs = (DfsService) registry.lookup(name);
        } catch (Exception e) {
            System.err.println("ComputePi exception:");
            e.printStackTrace();
        }
        
	}
	
	public void addFileToDFS(String path) {
		
	}
}
