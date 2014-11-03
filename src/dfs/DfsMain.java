/**
 * The main class for the distributed file system.
 */
package dfs;

import java.io.BufferedReader;
import java.io.FileReader;

public class DfsMain
{    
    public static void main(String[] args) throws Exception {
        FileReader fr = new FileReader("src/dfs/tempDfsConfigFile");        
        BufferedReader br = new BufferedReader(fr);
        String s = br.readLine();
        String path = s.split("=")[1];
        
    }
}
