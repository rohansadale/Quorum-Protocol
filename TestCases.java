import java.io.*;
import java.util.*;
import java.text.DecimalFormat;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestCases
{
	private static String CONFIG_FILE_NAME				= "";
	private static String FILE_KEY						= "Files";
	private static String QUORUM_READ_KEY				= "QuorumRead";
	private static String QUORUM_WRITE_KEY				= "QuorumWrite";
	private static String FILE_DIR_KEY					= "FileDirectory";
	private static String COORDINATOR_IP_KEY			= "CoordinatorIP";
	private static String COORDINATOR_PORT_KEY			= "CoordinatorPort";
	private static String CURRENT_NODE_IP				= "";

	public static void main(String []targs)
	{
		try
		{
			CURRENT_NODE_IP			= InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e)
		{
			System.out.println("Unable to get hostname ....");
		}

		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);
		if(targs.length < 2)
		{
			System.out.println("Please enter proper command line arguments. Pass config.txt and type of load you want 0 => Equal, 1 => Read-Heavy, 2 => Write heavy");
			return;
		}		
		
		CONFIG_FILE_NAME								= targs[0];
		int loadType									= Integer.parseInt(targs[1]);
		HashMap<String,String> configParam  			= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		String [] filenames								= configParam.get(FILE_KEY).split(",");
		String directory								= configParam.get(FILE_DIR_KEY);
		String baseCmd									= "java -cp .:../jars/libthrift-0.9.1.jar:../jars/slf4j-api-1.7.14.jar:gen-java/ Client config.txt";
		String baseCmdRead								= baseCmd + " 0 ";
		String baseCmdWrite								= baseCmd + " 1 ";
		String Write									= "append";
	
		int writes										= 0;
		int reads										= 0;
		long writeTime									= 0;
		long readTime									= 0;
		int status										= 0;
		int tc											= 100;
		String command									= "";

		for(int i=0;i<tc;i++)
		{
			if(loadType == 0)
				status		= rnd.nextInt(2);
			else if(loadType == 1)
				status		= rnd.nextInt(5);
			else
			{
				status		= rnd.nextInt(5);
				if(status<4) status = 0; //making write-heavy 
			}
			
			if(0==status)
			{
				writes++;
				String filename = filenames[rnd.nextInt(filenames.length)];
				try
				{
					FileWriter	fw = new FileWriter(directory+filename,true);
					fw.write(new BigInteger(130,new SecureRandom()).toString(32) +  " From " + CURRENT_NODE_IP);
					fw.write("\n");
					fw.close();
					command	= baseCmdWrite + filename;
				}
				catch(IOException e) {}
			}
			else
			{
				reads++;
				command	= baseCmdRead + filenames[rnd.nextInt(filenames.length)];
			}
			
			try
			{
				System.out.println("Running command " + command);
				Runtime r = Runtime.getRuntime();
				long before	= System.currentTimeMillis();
				Process p = r.exec(command);
				long after  = System.currentTimeMillis();

				if(0==status) writeTime	= writeTime + after-before;
				else readTime	= readTime + after-before;

				BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line = "";
	
				while ((line = b.readLine()) != null) {
  					System.out.println(line);
				}
				b.close();
			}
			catch(IOException e) {}
		}
		
		DecimalFormat df 				= new DecimalFormat("#.###");
		double avgReadTime				= ((double)readTime*1.0)/reads;
		double avgWriteTime				= ((double)writeTime*1.0)/writes;
		System.out.println("Average Read Time :- " + df.format(avgReadTime)  + " milli-seconds\nAverage Write Time :- " + df.format(avgWriteTime) + " milli-seconds");
	}
}
