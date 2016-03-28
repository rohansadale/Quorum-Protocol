import java.io.*;
import java.util.*;
import java.net.InetAddress;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import java.net.UnknownHostException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

/*
- This is the Coordinator file which reads configuration paramters from the configuration file provided as command line argument and Multi-Threaded Server which listens
to requests coming from different file-servers.
- Coordinator also runs background thread whose job is to sync content across all nodes in the system
*/

public class Coordinator
{
	private static String CONFIG_FILE_NAME				= "";	
	private static String CURRENT_NODE_IP				= "";
	private static String FILE_KEY						= "Files";	
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";
	private static String SYNC_INTERVAL_KEY				= "SyncInterval";

	public static void main(String targs[]) throws TException
	{
		try
		{
			CURRENT_NODE_IP			= InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e)
		{
			System.out.println("Unable to get hostname ....");
		}

		if(CURRENT_NODE_IP=="")
		{
			System.out.println("Unable to get Current System's IP");
			return;
		}
		if(targs.length==1)
		{
			CONFIG_FILE_NAME					= targs[0];
		}
		else
		{
			System.out.println("Config file missing!!!");
			return;
		}
		
		HashMap<String,String> configParam	= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		String hashKey						= CURRENT_NODE_IP + configParam.get(COORDINATOR_PORT_KEY);
		Node currentNode					= new Node(CURRENT_NODE_IP,Integer.parseInt(configParam.get(COORDINATOR_PORT_KEY)),Util.getInstance().hash(hashKey));
		QuorumServiceHandler quorum			= new QuorumServiceHandler(currentNode,currentNode,configParam.get(FILE_DIR_KEY),
																		configParam.get(FILE_KEY).split(","),
																		Integer.parseInt(configParam.get(QUORUM_READ_KEY)),
																		Integer.parseInt(configParam.get(QUORUM_WRITE_KEY)),
																		Integer.parseInt(configParam.get(SYNC_INTERVAL_KEY)));
		//Initializing the server
		TThreadPoolServer server			= Util.getInstance().getQuorumServer(Integer.parseInt(configParam.get(COORDINATOR_PORT_KEY)),quorum);	
		quorum.syncJob(); //Starts a thread to sync job across different machines
		server.serve();
	}	
}
