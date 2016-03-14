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

public class QuorumServiceHandler implements QuorumService.Iface
{
	List<Node> activeNodes;
	private static String CURRENT_NODE_IP	= "";
	private static int CURRENT_NODE_PORT		= 0;

	public QuorumServiceHandler(Node currentNode)
	{
		activeNodes		= new ArrayList<Node>();
		CURRENT_NODE_IP	= currentNode.ip;
		CURRENT_NODE_PORT= currentNode.port;
		activeNodes.add(currentNode);
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
	public boolean update(List<Node> currentNodes)
	{
		System.out.println("Updating node list at "+ CURRENT_NODE_IP + " and port " + CURRENT_NODE_PORT);
		activeNodes	= currentNodes;
		Util.printNodeList(activeNodes);
		return true;
	}

	@Override
	public List<Node> getNodes(int numNodes) throws TException
	{
		System.out.println(activeNodes==null);
		Collections.shuffle(activeNodes);
		List<Node> result	= new ArrayList<Node>();
		for(int i=0;i<Math.min(activeNodes.size(),numNodes);i++)
			result.add(activeNodes.get(i));
		return result;
	}

	@Override
    public String read(String filename,String readDirectory,boolean shouldRead) throws TException
	{
		if(shouldRead) return Util.getFileContent(readDirectory+filename);
		return Util.getMaxVersion(filename,readDirectory);
	}

    @Override
	public boolean write(String filename,String writeDirectory,String content) throws TException
	{
		String maxVersion	= Util.getMaxVersion(filename,writeDirectory);
		int nextVersion		= Integer.parseInt(maxVersion)+1;
		String nfileName	= filename + "." + String.valueOf(nextVersion);
		System.out.println("Write request received at " + CURRENT_NODE_IP+":"+CURRENT_NODE_PORT);
		return Util.writeContent(writeDirectory+nfileName,content);
	}
	
	@Override
	public Node GetNode() throws TException
	{
		System.out.println("Selecting random Node ....");
		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);
		return activeNodes.get(rnd.nextInt(activeNodes.size()));
	}
}
