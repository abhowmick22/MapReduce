package clientapi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import dfs.DfsService;

public class ClientApi_Impl implements ClientApi {

	private int _registryPort;			//registry port 
	private String _registryHost;       //registry host
	
	public ClientApi_Impl() {
		
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		FileReader fr = null;
        try {
            fr = new FileReader("src/dfs/tempDfsConfigFile");   //TODO: change the name
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line=br.readLine())!=null) {  
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                //check which key has been read, and initialize the appropriate global variable
                if(keyValue[0].equals("RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                } else if(keyValue[0].equals("RegistryHost")) {
                    _registryHost = keyValue[1].replaceAll("\\s", "");
                } else {
                    System.out.println("Registry port not found. Program exiting..");
                    System.exit(0);
                }                
            }
            String name = "DfsService";
            Registry registry = LocateRegistry.getRegistry(_registryHost, _registryPort);
            DfsService dfs = (DfsService) registry.lookup(name);
        } catch (Exception e) {
            System.err.println("DfsService exception:");
            e.printStackTrace();
        }
        
	}
	
	@Override
	public void addFileToDFS(String path) {
		
	}
	
}
