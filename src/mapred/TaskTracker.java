package mapred;

/*
 * Objects of this class run on the slave nodes in the cluster (Datanode), they communicate with the
 * JobTracker on the master node
 * It receives task jobs from the JobTracker and run them in different threads
 * It maintains a thread pool
 *  - If threads are available in the pool, it accepts the task and sends an ACK
 *  - If no threads are available, it rejects the task and sends back a NACK
 */

public class TaskTracker {

}
