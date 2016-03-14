import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TServer;
import java.net.UnknownHostException;
import org.apache.thrift.transport.*;
import java.net.InetAddress;
import java.io.*;

public class Client
{
	private static String CONFIG_FILE_NAME				= "";
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";
	private static String COORDINATOR_IP_KEY			= "CoordinatorIP";
	private static int COORDINATOR_PORT					= 0;
	private static String COORDINATOR_IP				= "";
	private static Node startNode						= null;
	private static ArrayList<Node> requiredNodes		= null;
	private static int OpType							= 0;
	private static String filename					 	= "";
	private static String content						= "";	
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static int NR								= 0;
	private static int NW								= 0;
	private static String CURRENT_NODE_IP				= "";

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
			setParameters();	
			OpType								= Integer.parseInt(targs[1]);
			filename							= targs[2];
			if(0 == OpType) content				= Util.getFileContent(filename);			
		}	

		try
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.client(protocol);
			transport.open();
			startNode							= client.GetNode();		
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
				transport.close();
				return;
		}	
		
		if(null==startNode)
		{
			System.out.println("Unable to connect to Node... Exiting ...");
			transport.close();
			return;
		}
		
		requiredNodes							= client.getNodes((OpType==0?NR:NW));	
		if(0==OpType)
		{
		}
		else
		{
			
		}		
		transport.close();
	}
	
	public static void setParameters()
	{
		String content;
		BufferedReader br	= null;
		try
		{
			br				= new BufferedReader(new FileReader(CONFIG_FILE_NAME));
			while((content == br.readLine())!=null)
			{
				String [] tokens 		= content.split(":");
				if(tokens.length==2 && tokens[0].equals(QUORUM_READ_KEY)==true)
					NR					= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(QUORUM_WRITE_KEY)==true)
					NW					= Integer.parseInt(tokens[1);
				if(tokens.length==2 && tokens[0].equals(COORDINATOR_PORT_KEY)==true)
					COORDINATOR_PORT	= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(COORDINATOR_IP_KEY)==true)
					COORDINATOR_IP		= Integer.parseInt(tokens[1);
			}
		}
		catch(IOException) {}
		finally
		{
			try
			{
				if(br!=null) br.close();
			}
			catch(IOException){}
		}
	}
}
