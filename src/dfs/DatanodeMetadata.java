/**
 * Stores the file names and associated metadata on each node.
 */
package dfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//TODO: thread safety for this is not actually necessary as long as all methods in DfsService_Impl are synchronized

public class DatanodeMetadata {
	
	//private String nodeName;							//name of the node
	private List<String> fileList;						//list of files stored on that node - stored in the form of DFS path
	private Map<String, List<String>> fileBlockMap;		//map from file's DFS path to list of block path names of that file on this node
	//blocks are stored in LocalBaseDir/username/ directory where LocalBaseDir is provided in the config file of the DFS.
	
	public DatanodeMetadata(String nodeName) {
		//this.nodeName = nodeName;
		fileList = new ArrayList<String>();				//TODO: make sure thread-safety
		fileBlockMap = new ConcurrentHashMap<String, List<String>>();
	}
	
//	public synchronized String getName() {
//		return this.nodeName;
//	}
	
	public synchronized List<String> getFileList() {
		return this.fileList;
	}
	
	public synchronized Map<String, List<String>> getFileBlockMap() {
		return this.fileBlockMap;
	}
	
//	public synchronized void setName(String nodeName) {
//		this.nodeName = nodeName;
//	}
	
	public synchronized void setFileList(List<String> fileList) {
		this.fileList = fileList;
	}
	
	public synchronized void setFileBlockMap(Map<String, List<String>> fileBlockMap) {
		this.fileBlockMap = fileBlockMap;
	}
	
}
