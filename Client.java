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

public class Client
{
	private static String CONFIG_FILE_NAME				= "";
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";
	private static String COORDINATOR_IP_KEY			= "CoordinatorIP";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String FILE_DIR						= "";
	private static int COORDINATOR_PORT					= 0;
	private static String COORDINATOR_IP				= "";
	private static Node startNode						= null;
	private static List<Node> requiredNodes				= null;
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

		String currentVersion					= "0";
		String maxVersion						= "0";
		int requiredIdx							= -1;

		try
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
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

		System.out.println("Connecting to Node " + startNode.ip + " with port " + startNode.port + " with Optype " + OpType);
		//Connect with the nodes to get set of nodes for performing read/write op		
		try
		{
			TTransport conTransport					= new TSocket(startNode.ip,startNode.port);
			TProtocol conProtocol					= new TBinaryProtocol(new TFramedTransport(conTransport));
			QuorumService.Client conClient			= new QuorumService.Client(conProtocol);
			conTransport.open();	
			requiredNodes							= conClient.getNodes(OpType==0?NR:NW);	
			conTransport.close();
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Randomly selected Node ... Exiting ... ================="+x);
				return;
		}
		if(null==requiredNodes)
		{	
			System.out.println("Unable to obtain to set of nodes to perform the operation .... Exiting ...");
			return;
		}
		if(0==OpType)
		{
			for(int i=0;i<requiredNodes.size();i++)
			{
				System.out.println("Reading from " + requiredNodes.get(i).ip + " with port " + requiredNodes.get(i).port);
				TTransport readTransport			= new TSocket(requiredNodes.get(i).ip,requiredNodes.get(i).port);
				TProtocol readProtocol				= new TBinaryProtocol(new TFramedTransport(readTransport));
				QuorumService.Client readClient		= new QuorumService.Client(readProtocol);
				readTransport.open();
				currentVersion						= readClient.read(filename,FILE_DIR,false);
				readTransport.close();
				if(currentVersion.compareTo(maxVersion) > 0 )
				{
					maxVersion						= currentVersion;
					requiredIdx						= i;
				}
			}
			filename								= filename + "." + maxVersion;
			if(requiredIdx != -1)
			{
				TTransport freadTransport			= new TSocket(requiredNodes.get(requiredIdx).ip,requiredNodes.get(requiredIdx).port);
				TProtocol freadProtocol				= new TBinaryProtocol(new TFramedTransport(freadTransport));
				QuorumService.Client freadClient	= new QuorumService.Client(freadProtocol);
				freadTransport.open();
				String content						= freadClient.read(filename,FILE_DIR,true);
				freadTransport.close();
				System.out.println("Latest Content of file " + filename +" is " + content);
			}
			else System.out.println("File not Present!!!");
		}
		else
		{
			for(int i=0;i<requiredNodes.size();i++)
			{
				TTransport writeTransport			= new TSocket(requiredNodes.get(i).ip,requiredNodes.get(i).port);
				TProtocol writeProtocol				= new TBinaryProtocol(new TFramedTransport(writeTransport));
				QuorumService.Client writeClient    = new QuorumService.Client(writeProtocol);
				writeTransport.open();
				boolean hasWritten					= writeClient.write(filename,FILE_DIR,Util.getFileContent(FILE_DIR+filename),true);
				System.out.println("Writing to " + requiredNodes.get(i).ip + " with port " + requiredNodes.get(i).port);
				if(hasWritten==false)
				{
					System.out.println("Unable to write on node " + requiredNodes.get(i).ip + " with port " + requiredNodes.get(i).port);
				}
				writeTransport.close();
			}
		}		
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
				String [] tokens 		= content.split(":");
				if(tokens.length==2 && tokens[0].equals(QUORUM_READ_KEY)==true)
					NR					= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(QUORUM_WRITE_KEY)==true)
					NW					= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(COORDINATOR_PORT_KEY)==true)
					COORDINATOR_PORT	= Integer.parseInt(tokens[1]);
				if(tokens.length==2 && tokens[0].equals(COORDINATOR_IP_KEY)==true)
					COORDINATOR_IP		= tokens[1];
				if(tokens.length==2 && tokens[0].equals(FILE_DIR_KEY)==true)
					FILE_DIR			= tokens[1];
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
