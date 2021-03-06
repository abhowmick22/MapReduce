package mapred;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * The sole purpose of this thread is to indicate it is alive
 * Also, it is a daemon, so it goes down whenever TaskTracker goes down
 */

public class TTPolling implements Runnable{
	
	// server socket for responding to health report requests
	private static ServerSocket pollingSocket;

	public TTPolling(ServerSocket pollingSocket){
		TTPolling.pollingSocket = pollingSocket;
	}
	
	@Override
	public void run() {
		while(true){
			try {
				pollingSocket.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
