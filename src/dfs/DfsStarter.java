package dfs;

import java.io.BufferedReader;
import java.io.FileReader;

public class DfsStarter
{
    public static void main(String[] args) throws Exception {
        //This is where all the blocks are physically stored
        System.out.println("The base directory for our DFS is: src/dfs/dfsBaseDir");
        
        FileReader fr = new FileReader("src/dfs/tempDfsConfigFile");        
        BufferedReader br = new BufferedReader(fr);
        String s = br.readLine();
        String path = s.split("=")[1];
        
    }
}
