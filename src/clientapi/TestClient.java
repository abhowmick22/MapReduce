package clientapi;

import java.net.InetAddress;

import dfs.InputSplit;

public class TestClient
{   //TODO: this class should not be in this package.
    
    public static void main(String[] args) throws Exception {
        ClientApi capi = new ClientApi_Impl();
        String hostname = InetAddress.getLocalHost().getHostName();
        InputSplit inputSplit = new InputSplit('\n');
        capi.addFileToDfs("test/world95.txt", "/dfs/"+hostname+"/world95.txt", inputSplit, true);
        System.out.println(capi.printDFSStructure());
        capi.getFileFromDfs("/dfs/"+hostname+"/world95.txt", "testOP/");
        //capi.deleteFileFromDfs("/dfs/"+hostname+"/world95.txt");
    }
}
 
//number of blocks (64 MB per block): (int)Math.ceil((double)(new File("ubuntu/askubuntu.com/PostHistory.xml").length())/1024/1024/64)