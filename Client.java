import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TServer;
import java.net.UnknownHostException;
import org.apache.thrift.transport.*;
import java.net.InetAddress;
import java.io.*;
import java.util.*;
import org.apache.thrift.TException;

/*
This is the client file. It actually connects randomly with any of the file-server and sends the operation to that file-server
This file takes 3 command line parameters
	* Configuration file
	* Optype => 0 for read and 1 for write
	* filename => only filename not complete path which is to be read or written
If optype is 1(i.e write operation) this script will read contents of the file and that content will be written on files in the network
*/
public class Client
{
	private static String CONFIG_FILE_NAME				= "";
	private static String FILE_DIR						= "";
	private static int COORDINATOR_PORT					= 0;
	private static String COORDINATOR_IP				= "";
	private static Node startNode						= null;
	private static List<Node> requiredNodes				= null;
	private static int OpType							= 0;
	private static String filename					 	= "";
	private static String content						= "";	
	private static String CURRENT_NODE_IP				= "";
	private static String FILE_KEY						= "Files";
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String COORDINATOR_IP_KEY			= "CoordinatorIP";
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";

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

		if(targs.length>=3)
		{
			CONFIG_FILE_NAME					= targs[0];
			OpType								= Integer.parseInt(targs[1]);
			filename							= targs[2];
		}	

		HashMap<String,String> configParam  	= Util.getInstance().getParameters(CONFIG_FILE_NAME);		
		if(1 == OpType) content					= Util.getInstance().getFileContent(configParam.get(FILE_DIR_KEY)+filename);			
		try
		{
			//Randomly connecting to the node in the network
			TTransport transport				= new TSocket(configParam.get(COORDINATOR_IP_KEY),Integer.parseInt(configParam.get(COORDINATOR_PORT_KEY)));
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.Client(protocol);
			transport.open();
			startNode							= client.GetNode();	
			transport.close();
		}

		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
				return;
		}	
		
		if(null==startNode)
		{
			System.out.println("Unable to connect to Node... Exiting ...");
			return;
		}
	
		System.out.println("Initially Connected to :- " + startNode.ip + ":" + startNode.port);	
		Job job									= new Job(startNode,1,OpType,filename,content); //Building the job object
		TTransport transport					= new TSocket(startNode.ip,startNode.port); //Creating connection with required file-server
		TProtocol Protocol						= new TBinaryProtocol(new TFramedTransport(transport));
		QuorumService.Client Client				= new QuorumService.Client(Protocol);
		transport.open();
		JobStatus status						= Client.submitJob(job); //Submitting the job
		transport.close();
		if(status == null) 
		{
			System.out.println("Unable to finish the job"); 
		}
		else
		{
			//Printing out information about the job
			System.out.println("Job Status :- " + status.status);
			System.out.println("Content :- " + status.content);
			if(status.path != null )
			{
				System.out.println("Following nodes were contacted :- ");
				for(int i=0;status.path != null && i<status.path.size();i++)
					System.out.println(status.path.get(i).ip + ":" + status.path.get(i).port);
			}
		}
	}
}
