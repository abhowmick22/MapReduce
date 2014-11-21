package clientapi;

import java.net.InetAddress;

import dfs.InputSplit;

public class TestClient
{   //TODO: this class should not be in this package.
    
    public static void main(String[] args) throws Exception {
        ClientApi capi = new ClientApi_Impl();
        String hostname = InetAddress.getLocalHost().getHostName();
        InputSplit inputSplit = new InputSplit(80);
        if(!capi.checkFileExists("/dfs/"+hostname+"/world95.txt"))
            capi.addFileToDfs("test/world95.txt", "/dfs/"+hostname+"/world95.txt", inputSplit, false);   
//        capi.runMapReduce("Jar path", "Dfs path for input file", "Dfs path for output", "numbr of reducers", 
//                "job name", "username of user");
        System.out.print(capi.printDFSStructure());
        Thread.sleep(2000);
        //capi.getFileFromDfs("Dfs path for output", "testOP/");
        capi.getDirFromDfs("/dfs/"+hostname, hostname);
        //capi.deleteFileFromDfs("/dfs/"+hostname+"/world95.txt");
    }
}
 
//number of blocks (64 MB per block): (int)Math.ceil((double)(new File("ubuntu/askubuntu.com/PostHistory.xml").length())/1024/1024/64)