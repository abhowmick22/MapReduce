package clientapi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.security.Policy;

import dfs.DfsService;

public class ClientApi_Impl implements ClientApi {

	private int _registryPort;			//registry port 
	private String _registryHost;       //registry host
	
	public ClientApi_Impl() {
		
		if (System.getSecurityManager() == null) {
		    System.setProperty("java.security.policy", "client.policy");
            System.setSecurityManager(new SecurityManager());
        }
		
		FileReader fr = null;
        try {
            fr = new FileReader("tempDfsConfigFile");   //TODO: change the name
            BufferedReader br = new BufferedReader(fr);
            _registryPort = -1;
            _registryHost = "";
            String line;
            while((line=br.readLine())!=null) {  
                if(line.charAt(0) == '#') {
                    //comment in config file
                    continue;
                }
                String[] keyValue = line.split("=");
                String key = keyValue[0].replaceAll("\\s", "");
                //check which key has been read, and initialize the appropriate global variable
                if(key.equals("RegistryPort")) {
                    _registryPort = Integer.parseInt(keyValue[1].replaceAll("\\s", ""));
                } else if(key.equals("RegistryHost")) {
                    _registryHost = keyValue[1].replaceAll("\\s", "");
                }                
            }
            if(_registryPort == -1 || _registryHost.equals("")) {
                System.out.println("Registry port/host not found. Program exiting..");
                System.exit(0);
            } 
            String name = "DfsService";
            Registry registry = LocateRegistry.getRegistry(_registryHost, _registryPort);
            System.out.println(registry.list()[0]);
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
