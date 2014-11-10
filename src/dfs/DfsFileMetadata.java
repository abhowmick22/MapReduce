/**
 * DfsMetadata is used to store metadata about files in a particular directory in the DFS.
 */
package dfs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//TODO: thread safety for this is not actually necessary as long as all methods in DfsService_Impl are synchronized

public class DfsFileMetadata 
{
    private String name;
    private String user;
    private String permissions;
    //map between block names and list of machines to which the each of the blocks of this file are assigned
    private Map<String, List<String>> blocks;
    //map between blockname-nodename and a boolean value to signify if the block has successfully been
    //transferred to that node
    private Map<String, Boolean> blockConfirm;
    
    public DfsFileMetadata() {
        this.blocks = new ConcurrentHashMap<String, List<String>>();
        this.blockConfirm = new ConcurrentHashMap<String, Boolean>();
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
    
    public synchronized void setBlocks(Map<String, List<String>> blocks) {
        this.blocks = blocks;
    }
    
    public synchronized void setBlockConfirm(Map<String, Boolean> blockConfirm) {
        this.blockConfirm = blockConfirm;
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
    
    public synchronized Map<String, List<String>> getBlocks() {
        return this.blocks;
    }
    
    public synchronized Map<String, Boolean> getBlockConfirm() {
        return this.blockConfirm;
    }
    
}
