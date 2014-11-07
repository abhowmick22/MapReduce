package mapred;

/*
 * A runnable object of this type runs on the datanode as a daemon
 * It periodically checks the status of all the computing tasks running on this node (and also 
 * collects information about the state of the file system data), compiles everything into a health report
 * and sends it to the JobTracker
 */

public class TMonitor implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
