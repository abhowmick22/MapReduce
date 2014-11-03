package clientapi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class ConnectionEstablisher
{
    private static final String user = "ndhruva"; // TODO: Username of ssh account on remote machine
    private static final String host = "ghc81.ghc.andrew.cmu.edu"; // TODO: Hostname of the remote machine (eg: inst.eecs.berkeley.edu)
    private static final String password = ""; // TODO: Password associated with your ssh account
    private static final String command = "ls -l\n"; // Remote command you want to invoke
    
    public static void main(String args[]) throws JSchException, InterruptedException
    {
      JSch jsch = new JSch();
      
      // TODO: You will probably want to use your client ssl certificate instead of a password
      // jsch.addIdentity(new File(new File(new File(System.getProperty("user.home")), ".ssh"), "id_rsa").getAbsolutePath());

      Session session = jsch.getSession(user, host, 22);

      // TODO: You will probably want to use your client ssl certificate instead of a password
      session.setPassword(password);

      // Not recommended - skips host check
      session.setConfig("StrictHostKeyChecking", "no");

      // session.connect(); - ten second timeout
      session.connect(10*1000);

      Channel channel = session.openChannel("shell");

      // TODO: You will probably want to use your own input stream, instead of just reading a static string.
      InputStream is = new ByteArrayInputStream(command.getBytes());
      channel.setInputStream(is);
      
      // Set the destination for the data sent back (from the server)
      // TODO: You will probably want to send the response somewhere other than System.out
      channel.setOutputStream(System.out);

      // channel.connect(); - fifteen second timeout
      channel.connect(15 * 1000);
      
      // Wait three seconds for this demo to complete (ie: output to be streamed to us).
      Thread.sleep(3*1000);
      
      // Disconnect (close connection, clean up system resources)
      channel.disconnect();
      session.disconnect();
    }
  } 
