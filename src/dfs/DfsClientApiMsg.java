/**
 * Object of this class is used to communicate between the Client API and the DFS.
 */
package dfs;

import java.io.Serializable;

public class DfsClientApiMsg implements Serializable
{

    /**
     * Serial Version UID
     */
    private static final long serialVersionUID = 8987472079863955241L;
    
    public String inPath;   //input path in DFS
    public String outPath;  //output path in DFS
    
    public DfsClientApiMsg(String inPath, String outPath) {
        this.inPath = inPath;
        this.outPath = outPath;       
    }
    
    public void setInPath(String inPath) {
        this.inPath = inPath;
    }
    
    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }
    
    public String getInPath() {
        return this.inPath;
    }
    
    public String getOutPath() {
        return this.outPath;
    }
    
}
