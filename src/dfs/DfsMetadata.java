/**
 * DfsMetadata is used to store metadata about files in a particular directory in the DFS.
 */
package dfs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DfsMetadata 
{
    private String name;
    private String user;
    private String permissions;
    //list of machines to which the each of the blocks of this file are assigned
    private Map<Integer, List<String>> blocks;	
    
    public DfsMetadata() {
        this.blocks = new ConcurrentHashMap<Integer, List<String>>();
    }
    
    public synchronized void setName(String name) {
        this.name = name;
    }
    
    public synchronized void setUser(String user) {
        this.user = user;
    }
    
    public synchronized void setPermissions(String permissions) {
        this.permissions = permissions;
    }
    
    public synchronized void setBlocks(Map<Integer, List<String>> blocks) {
        this.blocks = blocks;
    }
    
    public synchronized String getName() {
        return this.name;
    }
    
    public synchronized String getUser() {
        return this.user;
    }
    
    public synchronized String getPermissions() {
        return this.permissions;
    }
    
    public synchronized Map<Integer, List<String>> getBlocks() {
        return this.blocks;
    }
    
}
