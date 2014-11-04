/**
 * Object of this class is used to communicate between the Client API and the DFS.
 */
package dfs;

import java.io.Serializable;

public class DfsClientApiMessage implements Serializable
{

    /**
     * Serial Version UID
     */
    private static final long serialVersionUID = 8987472079863955241L;
    
    public String inPath;   //input path in DFS
    public String outPath;  //output path in DFS
    
}
