package mapred;

/*
 * A Runnable object of this type runs on the namenode as a daemon
 * It contains a serversocket to listen to health reports from the task trackers
 * 
 * Create a constructor passing in objects from JobTracker that this monitor 
 * thread would like to access on receiving messages
 */

public class JTMonitor implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
