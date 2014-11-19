/**
 * DfsMetadata is used to store metadata about files in a particular directory in the DFS.
 */
package dfs;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//TODO: thread safety for this is not actually necessary as long as all methods in DfsService_Impl are synchronized

public class DfsFileMetadata implements Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -2389098731723487120L;
	private String name;
    private String user;
    private String permissions;
    private DfsStruct parentDfsStruct;
    //map between block names and list of machines to which the each of the blocks of this file are assigned
    private Map<String, List<String>> blocks;
    //map between "blockname--nodename" and a boolean value to signify if the block has successfully been
    //transferred to that node
    private Map<String, Boolean> blockAndNodeNameConfirm;
    
    public DfsFileMetadata() {
        this.blocks = new ConcurrentHashMap<String, List<String>>();
        this.blockAndNodeNameConfirm = new ConcurrentHashMap<String, Boolean>();
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
    
    public synchronized void setParentDfsStruct(DfsStruct dfsStruct) {
        this.parentDfsStruct = dfsStruct;
    }
    
    public synchronized void setBlocks(Map<String, List<String>> blocks) {
        this.blocks = blocks;
    }
    
    public synchronized void setBlockAndNodeNameConfirm(Map<String, Boolean> blockAndNodeNameConfirm) {
        this.blockAndNodeNameConfirm = blockAndNodeNameConfirm;
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
    
    public synchronized DfsStruct getParentDfsStruct() {
        return this.parentDfsStruct;
    }
    
    public synchronized Map<String, List<String>> getBlocks() {
        return this.blocks;
    }
    
    public synchronized Map<String, Boolean> getBlockAndNodeNameConfirm() {
        return this.blockAndNodeNameConfirm;
    }
    
}
