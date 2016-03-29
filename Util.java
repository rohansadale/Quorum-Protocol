import java.util.*;
import java.io.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Util
{
	private static int MOD 		= 107;
	private static Util util 	= null;
	
	//Creating Singleton instance of the class	
	public static Util getInstance()
	{
		if(util==null)
			util	= new Util();
		return util;
	}

	//Function to generate hash value for given string
	public static long hash(String input)
	{
		long hash = 5381;
		for (int i = 0; i < input.length() ;i++)
		{
			hash = ((hash << 11) + hash) + input.charAt(i)*26*(i+1);
			hash = hash%MOD;
		}
		return hash;		
	}

	//Function that takes port number and QuorumServiceObject as its paramter and returns TThreadPoolServer object
	public static TThreadPoolServer getQuorumServer(int Port,QuorumServiceHandler quorum) throws TTransportException
	{
		TServerTransport serverTransport    = new TServerSocket(Port);
        TTransportFactory factory           = new TFramedTransport.Factory();
        QuorumService.Processor processor   = new QuorumService.Processor(quorum);
        TThreadPoolServer.Args args         = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.transportFactory(factory);
		return new TThreadPoolServer(args);
	}

	//Utility function to read configuration file and returns hash-map containg paramters and their values
	public static HashMap<String,String> getParameters(String filename)
	{
		BufferedReader br	= null;
		String content		= "";
		HashMap<String,String> params	= new HashMap<String,String>();
		try
		{
			br				= new BufferedReader(new FileReader(filename));
			while((content = br.readLine())!=null)
			{
				String [] tokens 	= content.split(":");
				params.put(tokens[0],tokens[1]);
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
		return params;
	}	

	//Function that iterates over all the files in the directory and returns maximum version of the file that is available locally.	
	public static String getMaxVersion(String filename,String directory)
	{
		String maxVersion               = "0";
        File folder                     = new File(directory);
        File[] listOfFiles              = folder.listFiles();

        for(File file: listOfFiles)
        {
            if(file.isFile())
            {
                String tfileName        = file.getName();
                if(tfileName.contains(filename)==true)
                {
                    String [] tokens    = tfileName.split("\\.");
                    String curVersion   = (3==tokens.length ? tokens[tokens.length-1]:"0");
                    if(curVersion.compareTo(maxVersion) > 0 )
                        maxVersion      = curVersion;
                }
            }
        }
        return maxVersion;
	}

	//Function to read contents of the file
	public static String getFileContent(String filename)
	{
		String content;
        BufferedReader br       = null;
        StringBuilder sb        = new StringBuilder();
		if(new File(filename).exists() == false) return "NIL";
        try
        {
            br                  = new BufferedReader(new FileReader(filename));
            while((content = br.readLine()) != null)
                sb.append(content+"\n");
        }
        catch(IOException e) {}
        finally
        {
            try
            {
                if(br!=null) br.close();
            }
            catch(IOException e) {}
        }
        return sb.toString();
	}

	//Function to write content to the given file
	public static boolean writeContent(String filename,String content)
	{
		try
		{
        	BufferedWriter br   = new BufferedWriter(new FileWriter(filename));
        	br.write(content);
        	br.close();
        }
        catch(IOException e)
        {
        	return false;
       	}
        return true;
	}

	//Utility function to print nodes that are curently part of the system
	public static void printNodeList(List<Node> activeNodes)
	{
		System.out.println("Currently Nodes connected to Coordinator ... ");
		System.out.println("---------------------------------------------------------");
		System.out.println("        HostName               Port      NodeId          ");
		System.out.println("---------------------------------------------------------");
		for(int i=0;i<activeNodes.size();i++)
		{
			System.out.println(activeNodes.get(i).ip + "    " + activeNodes.get(i).port + "        " + activeNodes.get(i).id);
			System.out.println("---------------------------------------------------------");
		}
	}

	/*
	Function to sync data across nodes that are part of the network
	This function is quite big but what it essentially does is that for every file it established connection with all the nodes to get the latest content and version number
	and after getting latest version it replicates the content across all the nodes that don't have latest copy
	*/	
	public static void syncData(List<Node> activeNodes,String directory,String [] filenames,
							String CoordinatorIP,int CoordinatorPort,ReentrantReadWriteLock lock,
							HashMap<Node,HashMap<String,Integer> > versions) throws TTransportException,TException 
	{
		System.out.println("Backgorund sync started !!!!!");
		for(int i=0;i<filenames.length;i++)
		{
			System.out.println("\n===================================================\n");
			System.out.println("Syncing file " + filenames[i] + " across all nodes");
			String maxVersion		= "0";
			int requiredIdx			= -1;
			String currentVersion	= "";	
			try
			{
				lock.readLock().lock();
				for(int j=0;j<activeNodes.size();j++)
				{
					if(activeNodes.get(j).ip.equals(CoordinatorIP)==true && CoordinatorPort==activeNodes.get(j).port)
						currentVersion	= getMaxVersion(filenames[i],directory);
					else
					{
						TTransport transport				= new TSocket(activeNodes.get(j).ip,activeNodes.get(j).port);
						TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
						QuorumService.Client client			= new QuorumService.Client(protocol);
						transport.open();
						currentVersion						= client.version(filenames[i],directory);
						transport.close();
					}
					if(currentVersion.compareTo(maxVersion) > 0 )
					{
						maxVersion						= currentVersion;
						requiredIdx						= j;
					}
					System.out.println("At Node " + activeNodes.get(j).ip+":"+activeNodes.get(j).port + " with version " + currentVersion);
				}
			}
			finally
			{
				lock.readLock().unlock();
			}	
			
			if(requiredIdx != -1 )
			{	
				String requiredFileName					= filenames[i] + "." + maxVersion;
				System.out.println("File name to be written " + requiredFileName);
				String content							= "";
				try
				{
					lock.readLock().lock();
					if(activeNodes.get(requiredIdx).ip.equals(CoordinatorIP)==true && CoordinatorPort==activeNodes.get(requiredIdx).port)
						content								= getFileContent(directory+requiredFileName); 
					else
					{
						TTransport transport                = new TSocket(activeNodes.get(requiredIdx).ip,activeNodes.get(requiredIdx).port);
                		TProtocol protocol                  = new TBinaryProtocol(new TFramedTransport(transport));
                		QuorumService.Client client         = new QuorumService.Client(protocol);
                		transport.open();
                		content 	                        = client.read(requiredFileName,directory);
                		transport.close();
					}
				}
				finally
				{
					lock.readLock().unlock();
				}
				try
				{
					lock.writeLock().lock();	
					for(int j=0;j<activeNodes.size();j++)
					{
						versions.get(activeNodes.get(j)).put(filenames[i],Integer.parseInt(maxVersion));
						if(j == requiredIdx) continue;
						if(activeNodes.get(j).ip.equals(CoordinatorIP)==true && CoordinatorPort==activeNodes.get(j).port)
							Util.writeContent(directory+requiredFileName,content);
						else
						{
							TTransport writeTransport			= new TSocket(activeNodes.get(j).ip,activeNodes.get(j).port);
							TProtocol writeProtocol				= new TBinaryProtocol(new TFramedTransport(writeTransport));
							QuorumService.Client writeClient    = new QuorumService.Client(writeProtocol);
							writeTransport.open();
							boolean hasWritten					= writeClient.write(requiredFileName,directory,content);
							System.out.println("Writing to " + activeNodes.get(j).ip + " with port " + activeNodes.get(j).port);
							if(hasWritten==false)
								System.out.println("Unable to write on node " + activeNodes.get(j).ip + " with port " + activeNodes.get(j).port);
							writeTransport.close();
						}
					}
				}	
				finally
				{
					lock.writeLock().unlock();
				}
			}
			System.out.println(filenames[i] + " successfully synced across all nodes");
			System.out.println("\n===================================================\n");
		}
	}
	
	public static void printSystem(HashMap<Node,HashMap<String,Integer> > versions)
	{
		System.out.println();
		System.out.println("Printing Node along with version of file that they have. Version 0 implies that file is not present on that server");
		for(Map.Entry< Node,HashMap<String,Integer> > entry: versions.entrySet())
		{
			System.out.print(entry.getKey().ip+":"+entry.getKey().port+ " :- ");
			for(Map.Entry< String,Integer > nestedEntry: entry.getValue().entrySet())
				System.out.print("[" + nestedEntry.getKey() + " " + nestedEntry.getValue() + "],");
			System.out.println();
		}
		System.out.println();
	}
}
