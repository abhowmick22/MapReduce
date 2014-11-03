/**
 * DfsNode acts as a node in the Trie that is used to represent the in-memory storage of the 
 * distributed file system. Each instance of DfsNode acts as a directory in the DFS file structure.
 */

package dfs;

import java.util.HashMap;
import java.util.Map;

public class DfsNode
{
    private String name;
    private Map<String, DfsNode> subDirs;
    private Map<String, DfsMetadata> files;
    
    public DfsNode() {
        this.subDirs = new HashMap<String, DfsNode>();
        this.files = new HashMap<String, DfsMetadata>();
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public Map<String, DfsNode> getSubDirsMap() {
        return this.subDirs;
    }
    
    public Map<String, DfsMetadata> getFilesMetadata() {
        return this.files;
    }
    
    
}
