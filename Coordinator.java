import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import java.io.*;
import org.apache.thrift.TException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Coordinator
{
	private static String CONFIG_FILE_NAME				= "";	
	private static String CURRENT_NODE_IP				= "";
	private static String FILE_KEY						= "Files";	
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String COORDINATOR_PORT				= "CoordinatorPort";

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
		String hashKey						= CURRENT_NODE_IP + configParam[COORDINATOR_PORT];
		Node currentNode					= new Node(CURRENT_NODE_IP,configParam[COORDINATOR_PORT],Util.getInstance().hash(hashKey));
		QuorumServiceHandler quorum			= new QuorumServiceHandler(currentNode,currentNode,configParam[FILE_DIR_KEY],
																		new configParam[FILE_KEY].split(","),
																		Integer.parseInt(config[QUORUM_READ_KEY]),
																		Integer.parseInt(config[QUORUM_WRITE_KEY]));
		TThreadPoolServer server			= Util.getInstance().getQuorumServer(CURRENT_NODE_PORT,quorum);	
		quorum.pollJobQueue();	
		quorum.syncJob();
		server.serve();
	}	
}
