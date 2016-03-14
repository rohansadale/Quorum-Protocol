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
	private static int PORT						= 0;
	private static String COORDINATOR_PORT		= "CoordinatorPort";
	private static String QUORUM_READ_KEY		= "QuorumRead";
	private static String QUORUM_WRITE_KEY		= "QuorumWrite";
	private static int NR						= 0;
	private static int NW						= 0;
	private static String CONFIG_FILE_NAME		= "";	
	private static QuorumService.Processor processor;
	private static String CURRENT_NODE_IP		= "";

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
			setParameters();
		}
		else
		{
			System.out.println("Config file missing!!!");
			return;
		}
		
		TServerTransport serverTransport 		= new TServerSocket(PORT);
		TTransportFactory factory				= new TFramedTransport.Factory();
		QuorumServiceHandler quorum				= new QuorumServiceHandler(new Node(CURRENT_NODE_IP,PORT,Util.hash(CURRENT_NODE_IP+PORT)));
		processor								= new QuorumService.Processor(quorum);
		TThreadPoolServer.Args args				= new TThreadPoolServer.Args(serverTransport);
		args.processor(processor);
		args.transportFactory(factory);
		System.out.println("Starting Coordinator ....");
		TThreadPoolServer server				= new TThreadPoolServer(args);
		server.serve();
	}	
	
	public static void setParameters()
	{
		String content;
		BufferedReader br	= null;
		
		try
		{
			br				= new BufferedReader(new FileReader(CONFIG_FILE_NAME));
			while((content = br.readLine())!=null)
			{
				String [] tokens 	= content.split(":");
				if(tokens.length==2 && tokens[0].equals(QUORUM_READ_KEY)==true)
					NR				= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(QUORUM_WRITE_KEY)==true)
					NW				= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(COORDINATOR_PORT)==true)
					PORT			= Integer.parseInt(tokens[1]);
			}
		}
		catch(IOException e) {}
		finally
		{
			try
			{
				if(br!=null) br.close();
			}
			catch(IOException e){}
		}
	}
}
