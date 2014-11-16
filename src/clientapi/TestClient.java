package clientapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import dfs.InputSplit;

public class TestClient
{   //TODO: this class should not be in this package.
    
    public static void main(String[] args) throws IOException {
        ClientApi capi = new ClientApi_Impl();
        File file = new File("test/world95.txt");
//        RandomAccessFile rad = new RandomAccessFile("test/t.txt", "r");               
//        InputSplit iSplit = new InputSplit(80);
//        capi.addFileToDFS("test/world95.txt", iSplit);        
//        rad.close();
    }
}
 
//number of blocks (64 MB per block): (int)Math.ceil((double)(new File("ubuntu/askubuntu.com/PostHistory.xml").length())/1024/1024/64)