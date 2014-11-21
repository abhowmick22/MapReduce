package mapred;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;

import mapred.types.JobTableEntry;
import mapred.types.Pair;
import mapred.types.TaskTableEntry;

/*
 * This thread keeps polling the cluster Nodes to see if they are alive/
 * come online/etc. It also makes updates to mapredJobs and activeNodes
 */

public class JTPolling implements Runnable{
	
	// handle to main mapredJobs
	private static ConcurrentHashMap<Integer, JobTableEntry> mapredJobs;
	// handle to main activeNodes
	private static ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>> activeNodes;
	// list of all nodes in the cluster
	private static ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes;
	// port on which to poll slave nodes
	private static int pollingPort;
	// The IP Addr of the namenode
	private static String nameNode;
	// the port of the namenode
	private static int nameNodePort;
	
	public JTPolling(ConcurrentHashMap<Integer, JobTableEntry> mapredJobs, 
						ConcurrentHashMap<String, ArrayList<Pair<JobTableEntry, TaskTableEntry>>> activeNodes,
						ConcurrentHashMap<String, Pair<String, Integer>> clusterNodes,
						int pollingPort, String nameNode, int nameNodePort){
		JTPolling.mapredJobs = mapredJobs;
		JTPolling.activeNodes = activeNodes;
		JTPolling.clusterNodes = clusterNodes;
		JTPolling.pollingPort = pollingPort;
		JTPolling.nameNode = nameNode;
		JTPolling.nameNodePort = nameNodePort;
	}

	@Override
	public void run() {
		String poll = "ping";
		String prevStatus = null;
		Socket pollingSocket = null;
		ArrayList<String> activeNodeList = null;
		
		while(true){
			
		    //poll every 2.5s
			try {
				Thread.sleep(2500);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			activeNodeList = new ArrayList<String>();
			
			for(String currNode : JTPolling.clusterNodes.keySet()) {
				try {
					prevStatus = JTPolling.clusterNodes.get(currNode).getFirst();
					
					// ping this node on pollingPort
					pollingSocket = new Socket(currNode, JTPolling.pollingPort);
					ObjectOutputStream outStream = new ObjectOutputStream(pollingSocket.getOutputStream());
					outStream.writeObject(poll);
					outStream.flush();
					outStream.close();
					pollingSocket.close();
					
					// Coming here means that node was reachable
					JTPolling.clusterNodes.get(currNode).setFirst("up");
					//JTPolling.clusterNodes.get(currNode).setSecond(0);
					activeNodeList.add(currNode);
					
					if(prevStatus.equals("down")){			
						// dead node back again, meaning active nodes should be adding this back
						//System.out.println("Node " + currNode + " came back up.");
						JTPolling.activeNodes.put(currNode, new ArrayList<Pair<JobTableEntry, TaskTableEntry>>());
					}
				} catch (IOException e) {
					//System.out.println("Node " + currNode + " has gone down.");
					
					// Coming here means that the node is down
					JTPolling.clusterNodes.get(currNode).setFirst("down");
					JTPolling.clusterNodes.get(currNode).setSecond(0);
					
					// Use active nodes to mark its tasks invalid in mapredJobs
					if(prevStatus.equals("up")){
						ArrayList<Pair<JobTableEntry, TaskTableEntry>> deadTasks = JTPolling.activeNodes.get(currNode);
						System.out.println("Num of potential dead tasks is " + deadTasks.size());
						ListIterator<Pair<JobTableEntry, TaskTableEntry>> it = deadTasks.listIterator();
						while(it.hasNext()){
							Pair<JobTableEntry, TaskTableEntry> dt = it.next();
							// for debug, verify that each of the currNodeId of each of these entries is consistent
							//System.out.println("DEBUG: " + currNode + "\t" + dt.getSecond().getCurrNodeId());
							if(dt.getSecond().getStatus().equals("running")){
								dt.getSecond().setStatus("waiting");
								System.out.println("Set a running job to waiting.");
								// also do appropriate changes in the corresponding jobtableEntry, if any
									
							}
						}
						
						JTPolling.activeNodes.remove(currNode);
					}
				}
				
			}
			
			// TODO: Make call to namenode supplying list of activeNodes by calling updateActiveNodes
			
		}
		
	}

}
