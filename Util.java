import java.util.*;
import java.io.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class Util
{
	private static int MOD 		= 107;
	private static Util util 	= null;
	
	public static Util getInstance()
	{
		if(util==null)
			util	= new Util();
		return util;
	}

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
                sb.append(content);
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

	public static boolean writeContent(String filename,String content)
	{
		System.out.println("File name :- " + filename + " and content " + content);
		try
		{
        	BufferedWriter br   = new BufferedWriter(new FileWriter( filename));
        	br.write(content);
        	br.close();
        }
        catch(IOException e)
        {
        	return false;
       	}
        return true;
	}

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
	
	public static void syncData(List<Node> activeNodes,String directory,String [] filenames,String CoordinatorIP,int CoordinatorPort) throws TTransportException,TException 
	{
		for(int i=0;i<filenames.length;i++)
		{
			String maxVersion		= "0";
			int requiredIdx			= -1;
			String currentVersion	= "";	
			System.out.println("Starting syncing file " + filenames[i]);
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
			
			if(requiredIdx != -1 )
			{	
				String requiredFileName					= filenames[i] + "." + maxVersion;
				System.out.println("File name to be written " + requiredFileName);
				String content							= "";
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
				for(int j=0;j<activeNodes.size();j++)
				{
					if(j == requiredIdx) continue;
					System.out.println("Syncing data at " + activeNodes.get(j).ip+":"+activeNodes.get(j).port + " with content " + content);
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
						{
							System.out.println("Unable to write on node " + activeNodes.get(j).ip + " with port " + activeNodes.get(j).port);
						}
						writeTransport.close();
					}
				}
			}
			System.out.println(" ========================================================= " );
		}
	}
}
