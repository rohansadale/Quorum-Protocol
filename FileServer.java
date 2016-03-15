import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import org.apache.thrift.TException;
import java.io.*;

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
	
		boolean hasRegistered					= false;	
		try
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
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
		if(activeNodes!=null)
		{
			HashMap<String,String> configParam	= Util.getInstance().getParameters(CONFIG_FILE_NAME);
			String hashKey						= CURRENT_NODE_IP + CURRENT_NODE_PORT;
			String hashKeyCoordinator			= configParam[COORDINATOR_IP] + configParam[COORDINATOR_PORT];
			Node coordinatorNode				= new Node(configParam[COORDINATOR_IP],configParam[COORDINATOR_PORT],Util.getInstance().hash(hashKeyCoordinator));
			Node currentNode					= new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT,Util.getInstance().hash(hashKey));
			QuorumServiceHandler quorum			= new QuorumServiceHandler(coordinatorNode,currentNode,configParam[FILE_DIR_KEY],
																			new configParam[FILE_KEY].split(","),
																			Integer.parseInt(config[QUORUM_READ_KEY]),
																			Integer.parseInt(config[QUORUM_WRITE_KEY]));
			TThreadPoolServer server			= Util.getInstance().getQuorumServer(CURRENT_NODE_PORT,quorum);
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
