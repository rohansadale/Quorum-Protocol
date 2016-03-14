import org.apache.thrift.TException;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.lang.System;

public class QuorumServiceHandler implements QuorumService.Iface
{
	List<Node> activeNodes;
	public QuorumServiceHandler()
	{
		activeNodes		= new ArrayList<Node>();
	}

	@Override
	public boolean join(Node node) throws TException
	{
		activeNodes.add(node);
		return true;
	}
	
	@Override
	public ArrayList<Node> getNodes(int numNodes) throws TException
	{
		Collections.shuffle(activeNodes);
		ArrayList<Node> result	= new ArrayList<Node>();
		for(int i=0;i<numNodes;i++)
			result.add(activeNodes.get(i));
		return result;
	}

	@Override
    public String read(String filename,boolean shouldRead) throws TException
	{
		if(shouldRead) return Util.getFileContent(filename,readDirectory);
		return Util.getMaxVersion(filename,readDirectory);
	}

    @Override
	public boolean write(String filename,String content) throws TException
	{
		String maxVersion	= Util.getMaxVersion(filename,writeDirectory);
		int nextVersion		= Integer.parseInt(maxVersion)+1;
		String nfileName	= filename + "." + String.valueOf(nextVersion);
		return Util.writeContent(nfileName,writeDirectory,content);
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
