package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface DfsService extends Remote {

	public Map<String, List<String>> addFileToDfs(String path, String username, int numBlocks) throws RemoteException;
	public void confirmBlockReceipt(String blockAndNodeName);
	public Map<String, List<String>> deleteFileFromDfs(String path, String username) throws RemoteException;
	public String printDfsStructure();
	
}
