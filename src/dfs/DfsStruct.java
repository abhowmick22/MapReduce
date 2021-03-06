/**
 * DfsNode acts as a node in the Trie that is used to represent the in-memory storage of the 
 * distributed file system. Each instance of DfsNode acts as a directory in the DFS file structure.
 */

package dfs;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//TODO: thread safety for this is not actually necessary as long as all methods in DfsService_Impl are synchronized

public class DfsStruct implements Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -2824769180549937089L;
	private String name;                    //name of this node
    private String path;                    //entire path of this node
    private Map<String, DfsStruct> subDirs;
    private Map<String, DfsFileMetadata> files;
    
    public DfsStruct(String name) {
        this.name = name;
        //DFS is provided as a service, and hence  may be used by multiple users simultaneously
        this.subDirs = new ConcurrentHashMap<String, DfsStruct>();
        this.files = new ConcurrentHashMap<String, DfsFileMetadata>();
        
    }
    
    public DfsStruct(String name, String path) {
        this.name = name;
        this.path = path;
        //DFS is provided as a service, and hence  may be used by multiple users simultaneously
        this.subDirs = new ConcurrentHashMap<String, DfsStruct>();
        this.files = new ConcurrentHashMap<String, DfsFileMetadata>();
        
    }
    
    public synchronized String getName() {
        return this.name;
    }
    
    public synchronized String getPath() {
        return this.path;
    }
    
    public synchronized void setName(String name) {
        this.name = name;
    }
    
    public synchronized void setPath(String path) {
        this.path = path;
    }
    
    public synchronized Map<String, DfsStruct> getSubDirsMap() {
        return this.subDirs;
    }
    
    public synchronized Map<String, DfsFileMetadata> getFilesInDir() {
        return this.files;
    }
    
    
}
