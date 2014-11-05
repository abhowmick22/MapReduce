/**
 * DfsNode acts as a node in the Trie that is used to represent the in-memory storage of the 
 * distributed file system. Each instance of DfsNode acts as a directory in the DFS file structure.
 */

package dfs;

import java.util.HashMap;
import java.util.Map;

public class DfsStruct
{
    private String name;                    //name of this node
    private String path;                    //entire path of this node
    private Map<String, DfsStruct> subDirs;
    private Map<String, DfsMetadata> files;
    
    public DfsStruct(String name, String path) {
        this.name = name;
        this.path = path;
        this.subDirs = new HashMap<String, DfsStruct>();
        this.files = new HashMap<String, DfsMetadata>();
        
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getPath() {
        return this.path;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public Map<String, DfsStruct> getSubDirsMap() {
        return this.subDirs;
    }
    
    public Map<String, DfsMetadata> getFilesMetadata() {
        return this.files;
    }
    
    
}
