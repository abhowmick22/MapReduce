/**
 * DfsMetadata is used to store metadata about files in a particular directory in the DFS.
 */
package dfs;

import java.util.ArrayList;
import java.util.List;

public class DfsMetadata
{
    private String name;
    private String user;
    private String permissions;
    private List<Integer> blocks;
    
    public DfsMetadata() {
        this.blocks = new ArrayList<Integer>();
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }
    
    public void setBlocks(List<Integer> blocks) {
        this.blocks = blocks;
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public String getPermissions() {
        return this.permissions;
    }
    
    public List<Integer> getBlocks() {
        return this.blocks;
    }
    
}
