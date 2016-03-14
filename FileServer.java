import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.*;

public class FileServer
{
	private static String CONFIG_FILE_NAME				= "";	
	private static String CURRENT_NODE_IP				= "";   //Current Node IP
	private static int CURRENT_NODE_PORT 				= 9091;
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";
	private static String COORDINATOR_IP_KEY			= "CoordinatorIP";
	private static int COORDINATOR_PORT					= 0;
	private static String COORDINATOR_IP				= "";
	private static ArrayList<Node> activeNodes			= null;

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

		if(targs.length>=1)
		{
			CURRENT_NODE_PORT					= Integer.parseInt(targs[0]);
			CONFIG_FILE_NAME					= targs[0];
			setParameters();
		}
		else
		{
			System.out.println("Config file missing!!!");
			return;
		}
		
		try
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.client(protocol);
			transport.open();
			boolean hasRegistered				= client.join(new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT,Util.hash(CURRENT_NODE_IP+CURRENT_NODE_PORT)));
			transport.close();
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
				return;
		}
		if(hasRegistered)
		{
			TServerTransport serverTransport 		= new TServerSocket(CURRENT_NODE_PORT);
			TTransportFactory factory				= new TFramedTransport.Factory();
			QuorumServiceHandler quorum				= new QuorumServiceHandler();
			processor								= new QuorumService.Processor(quorum);
			TThreadPoolServer.Args args				= new TThreadPoolServer.Args(serverTransport);
			args.processor(processor);
			args.transportFactory(factory);
			System.out.println("Starting fileServer at " + CURRENT_NODE_IP + " and Port " + CURRENT_NODE_PORT + "  ....");
			TThreadPoolServer server				= new TThreadPoolServer(args);
			server.serve();	
		}
		else
		{
			System.out.println("Unable to Register with Coordinator");
			return;
		}
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
