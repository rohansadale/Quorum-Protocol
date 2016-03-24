import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;
import java.util.concurrent.locks.Lock;
import org.apache.thrift.protocol.TProtocol;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.thrift.protocol.TBinaryProtocol;

/*
This file implements the interface
*/
public class QuorumServiceHandler implements QuorumService.Iface
{
	List<Node> activeNodes;
	private static String CURRENT_NODE_IP		= "";
	private static int CURRENT_NODE_PORT		= 0;
	private static String COORDINATOR_IP		= "";
	private static int COORDINATOR_PORT			= 0;
	private static int SLEEP_TIMEOUT			= 60000;	
	private static int ReadQuorum				= 0;
	private static int WriteQuorum				= 0;
	String [] filenames							= null;	
	String baseDirectory						= "";
	Queue<Job> jobQueue							= null;
	private final ReentrantLock lock 			= new ReentrantLock();
	
	/*
	Initializing the Service Handler with Configuration Parameters
	*/	
	public QuorumServiceHandler(Node coordinatorNode,Node currentNode,String directory,String[] filenames,int ReadQuorum,int WriteQuorum)
	{
		activeNodes				= new ArrayList<Node>();
		jobQueue				= new LinkedList<Job>();
		activeNodes.add(currentNode);
	
		this.CURRENT_NODE_IP	= currentNode.ip;
		this.CURRENT_NODE_PORT	= currentNode.port;
		this.COORDINATOR_IP		= coordinatorNode.ip;
		this.COORDINATOR_PORT	= coordinatorNode.port;
		this.baseDirectory		= directory;	
		this.ReadQuorum			= ReadQuorum;
		this.WriteQuorum		= WriteQuorum;
		this.filenames			= new String[filenames.length];
		for(int i=0;i<filenames.length;i++)
			this.filenames[i]	= filenames[i];
	}

	/*
	Function to get randomly selected numNodes from the system
	*/
	public List<Node> getNodes(int numNodes) throws TException
	{
		Collections.shuffle(activeNodes); //shuffing the list and selecting top numNodes from the collection
		List<Node> result	= new ArrayList<Node>();
		for(int i=0;i<Math.min(activeNodes.size(),numNodes);i++)
			result.add(activeNodes.get(i));
		return result;
	}
	
	/*
	Background thread that actuall performs task of synchronizing the content among different replicas
	*/
	public void syncJob() 
	{
		Runnable syncThread = new Runnable() 
		{
         	public void run() 
			{
				while(true)
				{
        			try
					{
						Thread.sleep(SLEEP_TIMEOUT);
						Util.syncData(activeNodes,baseDirectory,filenames,CURRENT_NODE_IP,CURRENT_NODE_PORT);
					}
					catch(InterruptedException ex) {}
					catch(TTransportException ex) {}
					catch(TException ex){}
				} 
			}
     	};
		new Thread(syncThread).start();
	}

	/*
		Function that takes job to be performed as parameters and returns the status of the job
		This functions checks type of job i.e whether it is read or write and depending on that it contacts coordinator to perform the job
		This is where we have ensured sequential consistency i.e we have used locks so that at a time only one client could be executing the job
	*/
	private JobStatus processJob(Job job) throws TException,TTransportException
	{
		JobStatus result				= null;
		List<Node> requiredNodes	= getNodes(job.optype==0?ReadQuorum:WriteQuorum);
		String filename				= job.filename;
		String maxVersion			= "-1";
		String currentVersion		= "0";
		int requiredIdx				= -1;
		for(int i=0;i<requiredNodes.size();i++)
		{
			String ip			= requiredNodes.get(i).ip;
			int port			= requiredNodes.get(i).port;
			if(ip.equals(CURRENT_NODE_IP)==true && port == CURRENT_NODE_PORT) 
				currentVersion	= Util.getInstance().getMaxVersion(filename,baseDirectory);
			else
			{
				TTransport transport		= new TSocket(ip,port);
				TProtocol protocol			= new TBinaryProtocol(new TFramedTransport(transport));
				QuorumService.Client client = new QuorumService.Client(protocol);
				transport.open();
				currentVersion				= client.version(filename,baseDirectory);
				transport.close();
			}
			if(currentVersion.compareTo(maxVersion) > 0 )
			{
				maxVersion						= currentVersion;
				requiredIdx						= i;
			}	
		}
		result				= doJob(requiredNodes,job,maxVersion,requiredIdx);
		result.path			= requiredNodes;
		return result;
	}

	/*
	A utility function that takes 
		- List of the nodes
		- Job Object
		- MaxVersion of the file which is involved the the job
		- Index of node on which operation is to be performed (only used when operation is read)
	*/
	private JobStatus doJob(List<Node> nodes,Job job,String maxVersion,int idx) throws TException,TTransportException
	{
		if(idx==-1) return new JobStatus(false,"",null);
		String destFilename	= job.filename;
		
		if(job.optype==0) destFilename  = destFilename + "." + maxVersion;
        else destFilename   = destFilename + "." + (Integer.parseInt(maxVersion)+1);
		
		if(job.optype==0) return doReadJob(nodes.get(idx).ip,nodes.get(idx).port,destFilename);
		else
		{
			boolean status	= false;
			for(int i=0;i<nodes.size();i++) 
			{
				status		= status | doWriteJob(nodes.get(i).ip,nodes.get(i).port,destFilename,job.content);
				if(false == status) System.out.println(" ========= Unable to write on node : " + nodes.get(i).ip+":"+nodes.get(i).port + " =========== ");
			}
			return new JobStatus(status,"",null);
		}
	}

	/*
	Function that actually establishes TCP connection with required machine on the network and reads the file and returns the content of the file
	*/
	private JobStatus doReadJob(String ip,int port,String filename) throws TException,TTransportException
	{
		while(lock.isLocked()) {} //This is crucial as we don't need to ensure that no write is happening when we are reading;
		//Since we are not acquiring lock while reading concurrent reads are supported
		String content				= "";
    	TTransport transport        = new TSocket(ip,port);
        TProtocol protocol          = new TBinaryProtocol(new TFramedTransport(transport));
        QuorumService.Client client = new QuorumService.Client(protocol);
		transport.open();
		content 					= client.read(filename,baseDirectory);
		transport.close();
		return new JobStatus(true,content,null);
	}

	/*
	Function that actually establishes TCP connection with required machine on the network and writes the file on that machine
	*/
	private boolean doWriteJob(String ip,int port,String filename,String content) throws TException,TTransportException
	{
		boolean status				= false;
		try
        {
            lock.lock(); //This lock will ensure that writes are performed sequentially
            System.out.println("Lock Acquired !!!");
			TTransport transport        = new TSocket(ip,port);
        	TProtocol protocol          = new TBinaryProtocol(new TFramedTransport(transport));
        	QuorumService.Client client = new QuorumService.Client(protocol);
        	transport.open();
        	status 						= client.write(filename,baseDirectory,content);
        	transport.close();
		}
		finally
		{
			lock.unlock();
            System.out.println("Lock Released !!!");	
		}
		return status;
	}

	/*
	Function that is responsible for including the node in the network and returns list of nodes that are currently in the system.
	*/
	@Override
	public List<Node> join(Node node) throws TException
	{
		activeNodes.add(node);
		Collections.sort(activeNodes,new Comparator<Node>()
		{
			@Override
			public int compare(Node lhs,Node rhs)
			{
				if(lhs.id < rhs.id) return -1;
				else return 1;
			}
		});
		Util.printNodeList(activeNodes);	
		return activeNodes;
	}

	/*
	Function that returns randomly selected node from set of nodes that are in the network
	*/
	@Override
	public Node GetNode() throws TException
	{
		System.out.println("Selecting random Node ....");
		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);
		return activeNodes.get(rnd.nextInt(activeNodes.size()));
	}
	
	/*
	Function to get maximum version that is currently available
	*/
	@Override
	public String version(String filename,String directory) throws TException
	{
		return Util.getMaxVersion(filename,directory);
	}
	
	/*
	Function to read the file locally
	*/
	@Override
    public String read(String filename,String readDirectory) throws TException
	{
		System.out.println("Read Request Received at " + CURRENT_NODE_IP + ":" + CURRENT_NODE_PORT + " with filename " + filename);
		return Util.getFileContent(readDirectory+filename);
	}

	/*
	Function to write the file locally given filename and its content
	*/
    @Override
	public boolean write(String filename,String writeDirectory,String content) throws TException
	{
		System.out.println("Write request received at " + CURRENT_NODE_IP+":"+CURRENT_NODE_PORT + " with filename " + filename);
		return Util.writeContent(writeDirectory+filename,content);
	}
	
	/*
	Function that actually receives request from client and checks whether requests need to be forwarded to coordinator or current node is the coordinator
	If current node is coordinator then job is performed else connection to coordinator is established and then coordinator performs the job 
	*/
	@Override
	public JobStatus submitJob(Job job) throws TException,TTransportException
	{
		if(CURRENT_NODE_IP.equals(COORDINATOR_IP) == true && CURRENT_NODE_PORT == COORDINATOR_PORT)	
			return processJob(job);
		else
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.Client(protocol);
			transport.open();
			JobStatus status					= client.submitJob(job);
			transport.close();
			return status;
		}	
	}
}
