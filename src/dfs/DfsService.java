package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface DfsService extends Remote {

	public Map<String, List<String>> addFileToDfs(String path, String username, int numBlocks) throws RemoteException;
	public void confirmBlockAndNodeNameReceipt(String blockAndNodeName) throws RemoteException;
	public Map<String, List<String>> deleteFileFromDfs(String path, String username) throws RemoteException;
	public String printDfsStructure() throws RemoteException;
	public Map<String, List<String>> getFileFromDfs(String dfsPath, String username) throws RemoteException;
	public void nodeUpPing(String nodename) throws RemoteException;
	
}
