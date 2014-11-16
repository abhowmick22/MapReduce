package mapred;

/*
 * A Runnable object of this type runs on the namenode as a daemon
 * It contains a serversocket to listen to health reports from the task trackers
 * 
 * Create a constructor passing in objects from JobTracker that this monitor 
 * thread would like to access on receiving messages
 * 
 * It also accumulates task finish messages. On receiving such messages from 
 * slave node, it will append the output file info into the corresponding field
 * of the reduce task.
 * The format of the output file info is assumed to be <R:nodename:filepath>
 * where R = reducer number (starting from 1), nodename is name of machine,
 * filepath, is the path on that machine 
 */

public class JTMonitor implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
