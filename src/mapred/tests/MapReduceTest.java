package mapred.tests;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import clientapi.ClientApi;
import clientapi.ClientApi_Impl;
import dfs.InputSplit;

import mapred.interfaces.Mapper;
import mapred.interfaces.Reducer;
import mapred.messages.ClientAPIMsg;
import mapred.types.MapReduceJob;

/*
 * This class runs tests on the entire mapreduce flow on a single machine
 */

public class MapReduceTest {
	/***********************************************/
	public static void main(String[] args){
		
		ServerSocket JTsocket = null;
		Socket requestSocket = null;
		ObjectOutputStream requestStream = null;
		DefaultMapper map = new DefaultMapper();
		DefaultReducer reduce = new DefaultReducer();
		
		// Open up communications 
		try {		
			// open a new server socket for response from JobTracker
			JTsocket = new ServerSocket(20001);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Test 1. Send a launch request to JobTracker
				try {
					
					MapReduceJob job = new MapReduceJob();
					job.setIpFileName("/dfs/" + InetAddress.getLocalHost().getHostName() + "/word_count_small.txt");
					job.setJobName("Distributed Dummy");
					job.setMapper("DefaultMapper");
					job.setReducer("DefaultReducer");
					job.setIfCombiner(false);
					job.setNumReducers(1);
					job.setOpFileName("/dfs/" + InetAddress.getLocalHost().getHostName() + "/output.txt");
					job.setJarPath("test.jar");
					//job.setSplitSize(31457280);
					//job.setIpFileSize(125829120);
					//job.setJobId(42);
					ClientApi capi = new ClientApi_Impl();
			        String hostname = InetAddress.getLocalHost().getHostName();        
			        InputSplit inputSplit = new InputSplit(60);
			        if(!capi.checkFileExists("/dfs/"+hostname+"/world95.txt"))
			            capi.addFileToDfs("test/world95.txt", "/dfs/"+hostname+"/world95.txt", inputSplit, false);   
//			        capi.runMapReduce("Jar path", "Dfs path for input file", "Dfs path for output", "numbr of reducers", 
//			                "job name", "username of user");
			        //System.out.print(capi.printDFSStructure());
//			        capi.getFileFromDfs("Dfs path for output", "testOP/");
			        //capi.getDirFromDfs("/dfs/"+hostname, hostname);
			        //capi.deleteFileFromDfs("/dfs/"+hostname+"/world95.txt");
			        capi.startMapReduce("test.jar", "JarTest");
					
					
					
					requestSocket = new Socket("ghc51.ghc.andrew.cmu.edu", 20000);
					requestStream = new ObjectOutputStream(requestSocket.getOutputStream());
					ClientAPIMsg launchReq = new ClientAPIMsg();
					launchReq.setCommand("launchJob");
					launchReq.setJob(job);
					requestStream.writeObject(launchReq);
					requestStream.close();
					requestSocket.close();
					System.out.println("Sent a launch request");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	/***********************************************/
}
