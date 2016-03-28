import java.io.*;
import java.util.*;
import java.net.InetAddress;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;
import java.net.UnknownHostException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;

/*
This is file-server which runs a multi-threaded server and listens to multiple requests on client.
It takes command line paramter as configuration file and port number on which to listen to the requests.

*/
public class FileServer
{
	private static String CONFIG_FILE_NAME				= "";	
	private static String CURRENT_NODE_IP				= "";   //Current Node IP
	private static int CURRENT_NODE_PORT 				= 9091;
	private static String FILE_KEY						= "Files";
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String COORDINATOR_IP				= "CoordinatorIP";
	private static String COORDINATOR_PORT				= "CoordinatorPort";
	private static List<Node> activeNodes				= null;	
	private static String SYNC_INTERVAL_KEY             = "SyncInterval";

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

		if(targs.length>=2)
		{
			CURRENT_NODE_PORT					= Integer.parseInt(targs[1]);
			CONFIG_FILE_NAME					= targs[0];
		}
		else
		{
			System.out.println("Config file missing!!!");
			return;
		}
	
		HashMap<String,String> configParam	= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		boolean hasRegistered					= false;	
		try
		{
			//Establishing connection with coordinator and joining the system
			TTransport transport				= new TSocket(configParam.get(COORDINATOR_IP),Integer.parseInt(configParam.get(COORDINATOR_PORT)));
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.Client(protocol);
			transport.open();
			activeNodes							= client.join(new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT,Util.hash(CURRENT_NODE_IP+CURRENT_NODE_PORT)));
			transport.close();
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
				return;
		}
		if(activeNodes!=null) //checking whether current node is able to join network or not
		{
			String hashKey						= CURRENT_NODE_IP + CURRENT_NODE_PORT;
			String hashKeyCoordinator			= configParam.get(COORDINATOR_IP) + configParam.get(COORDINATOR_PORT);
			Node coordinatorNode				= new Node(configParam.get(COORDINATOR_IP),Integer.parseInt(configParam.get(COORDINATOR_PORT)),
													Util.getInstance().hash(hashKeyCoordinator));
			Node currentNode					= new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT,Util.getInstance().hash(hashKey));
			QuorumServiceHandler quorum			= new QuorumServiceHandler(coordinatorNode,currentNode,configParam.get(FILE_DIR_KEY),
																			configParam.get(FILE_KEY).split(","),
																			Integer.parseInt(configParam.get(QUORUM_READ_KEY)),
																			Integer.parseInt(configParam.get(QUORUM_WRITE_KEY)),
																			Integer.parseInt(configParam.get(SYNC_INTERVAL_KEY)));
			TThreadPoolServer server			= Util.getInstance().getQuorumServer(CURRENT_NODE_PORT,quorum);//Starting server to listen ot client requests
			System.out.println("Starting fileServer at " + CURRENT_NODE_IP + " and Port " + CURRENT_NODE_PORT + "  ....");
			server.serve();
		}
		else
		{
			System.out.println("Unable to Register with Coordinator");
			return;
		}
	}	
}
