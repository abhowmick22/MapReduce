package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface DfsService extends Remote {

	public Map<String, List<String>> addFileToDfs(String path, String username, int numBlocks, boolean overwrite) throws RemoteException;
	public void confirmBlockAndNodeNameReceipt(String blockAndNodeName) throws RemoteException;
	public void deleteFileFromDfs(String path, String username) throws RemoteException;
	public String printDfsStructure() throws RemoteException;
	public Map<String, List<String>> getFileFromDfs(String dfsPath, String username) throws RemoteException;
	public void updateActiveNodes(List<String> activeNodeList, String nodeListSentBy) throws RemoteException;
	public List<String> getBlocksOnNode(String nodename) throws RemoteException;
	public void reportFailedNode(String nodename, String dfsPath, String username) throws RemoteException;
	
}
