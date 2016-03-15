import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.lang.System;
import java.lang.Runnable;

public class QuorumServiceHandler implements QuorumService.Iface
{
	List<Node> activeNodes;
	private static String CURRENT_NODE_IP		= "";
	private static int CURRENT_NODE_PORT		= 0;
	private static String COORDINATOR_IP		= "";
	private static int COORDINATOR_PORT			= 0;
	private static int SLEEP_TIMEOUT			= 10000;	
	private static int ReadQuorum				= 0;
	private static int WriteQuorum				= 0;
	String [] filenames							= null;	
	String baseDirectory						= "";
	Queue<Job> jobQueue							= null;
		
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

	public List<Node> getNodes(int numNodes) throws TException
	{
		Collections.shuffle(activeNodes);
		List<Node> result	= new ArrayList<Node>();
		for(int i=0;i<Math.min(activeNodes.size(),numNodes);i++)
			result.add(activeNodes.get(i));
		return result;
	}
	
	public void pollJobQueue()
	{
		Runnable jobQueueThread	= new Runnable()
		{
			public void run()
			{
				while(true)
				{
					while(!jobQueue.empty())
					{
						Job	job	= jobQueue.peek();
						jobQueue.remove();
						final JobStatus	status	= processJob(job);
						//TODO::Notify	
					}
				}
			}
		}
		new Thread(jobQueueThread).start();
	}

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

	private void processJob(Job job)
	{
		List<Node> requiredNodes	= getNodes(job.optype==0?ReadQuorum:WriteQuorum);
		String filename				= job.filename;
		String maxVersion			= "0";
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
		return doJob(requiredNodes,job,maxVersion,requiredIdx);
	}

	private JobStatus doJob(List<Node> nodes,Job job,String maxVersion,int idx)
	{
		if(idx==-1) return new JobStatus(true,"");
		String destFilename	= job.filename;
		
		if(job.optype==0) destFilename  = destFilename + "." + maxVersion;
        else destFilename   = destFilename + "." + (Integer.parseInt(maxVersion)+1);
		
		if(job.optype==0) return doReadJob(nodes.get(idx).ip,nodes.get(idx).port,destFilename);
		else
		{
			boolean status	= false;
			for(int i=0;i<nodes.size();i++) 
			{
				status		= status | doWriteJob(nodes.get(i).ip,nodes.get(i).port,destFilename,optype.content);
				if(false == status) System.out.println(" ========= Unable to write on node : " + nodes.get(i).ip+":"+nodes.get(i).port + " =========== ");
			}
			return new JobStatus(status,"");
		}
	}

	private JobStatus doReadJob(String ip,int port,String filename)
	{
		String content				= "";
    	TTransport transport        = new TSocket(ip,port);
        TProtocol protocol          = new TBinaryProtocol(new TFramedTransport(transport));
        QuorumService.Client client = new QuorumService.Client(protocol);
		transport.open();
		content = client.read(destFilename,baseDirectory);
		transport.close();
		return new JobStatus(true,content);
	}

	private boolean doWriteJob(String ip,int port,String filename,String content)
	{
		boolean status				= false;
		TTransport transport        = new TSocket(ip,port);
        TProtocol protocol          = new TBinaryProtocol(new TFramedTransport(transport));
        QuorumService.Client client = new QuorumService.Client(protocol);
        transport.open();
        status 						= client.write(destFilename,baseDirectory,content);
        transport.close();
		return status;
	}

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

	@Override
	public Node GetNode() throws TException
	{
		System.out.println("Selecting random Node ....");
		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);
		return activeNodes.get(rnd.nextInt(activeNodes.size()));
	}
	
	@Override
	public String version(String filename,String directory) throws TException
	{
		return Util.getMaxVersion(filename,directory);
	}
	
	@Override
    public String read(String filename,String readDirectory) throws TException
	{
		System.out.println("Read Request Received at " + CURRENT_NODE_IP + ":" + CURRENT_NODE_PORT + " with filename " + filename);
		return Util.getFileContent(readDirectory+filename);
	}

    @Override
	public boolean write(String filename,String writeDirectory,String content) throws TException
	{
		System.out.println("Write request received at " + CURRENT_NODE_IP+":"+CURRENT_NODE_PORT + " with filename " + filename);
		return Util.writeContent(writeDirectory+filename,content);
	}
	
	@Override
	public JobStatus submitJob(Job job)
	{
		if(job.node.ip.equals(COORDINATOR_IP) == true && job.node.port == COORDINATOR_PORT)	jobQueue.add(job);
		else
		{
			TTransport transport				= new TSocket(COORDINATOR_IP,COORDINATOR_PORT);
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			QuorumService.Client client			= new QuorumService.Client(protocol);
			transport.open();
			activeNodes							= client.submitJob(job);
			transport.close();
		}	
	}
}
