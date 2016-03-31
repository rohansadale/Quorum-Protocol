import java.io.*;
import java.util.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.concurrent.*;
import java.text.DecimalFormat;
import java.security.SecureRandom;
import java.net.UnknownHostException;
/*
This file is used for testing system under various cirumstances i.e varying workload.
Paramters Nr and Nw can be set in config.txt
To Run this file provide with two command line parameters:-
	* configuration file path
	* type of work-load
		- 0 for equal read/write work load
		- 1 for read heavy work load
		- 2 write heavy workload

This script will run 100 operations combining read and write. If workload is set as read-heavy then approximately 80% of operations are read and if workload
is write-heavy then approximately 80% of operations are write and in equal workload case 50% are read and 50% write.
Script is completely randomized i.e while reading randomly file is selected among the set of feasible file  and while writing again random file is selected and random string is written and along with that string ip-address of client is also written. 
*/

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
	
		//Setting variables and reading parameters	
		CONFIG_FILE_NAME								= targs[0];
		int loadType									= Integer.parseInt(targs[1]);
		HashMap<String,String> configParam  			= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		String [] filenames								= configParam.get(FILE_KEY).split(",");
		String directory								= configParam.get(FILE_DIR_KEY);
		String baseCmd									= "java -cp .:../jars/libthrift-0.9.1.jar:../jars/slf4j-api-1.7.14.jar:gen-java/ Client config.txt";
		String baseCmdRead								= baseCmd + " 0 ";
		String baseCmdWrite								= baseCmd + " 1 ";
		String Write									= "append";

		long writeTime									= 0;
		long readTime									= 0;
		int writes										= 0;
		int reads										= 0;
		int status										= 0;
		int tc											= 100;
		String command									= "";
		ArrayList<WorkerThread> jobs					= new ArrayList<WorkerThread>();	
	
		for(int i=0;i<tc;i++)
		{
			//Depending upon load-type operation is set
			if(loadType == 0)
				status		= rnd.nextInt(2);
			else if(loadType == 1)
				status		= rnd.nextInt(10);
			else
			{
				status		= rnd.nextInt(10);
				if(status<9) status = 0; //making write-heavy 
			}
			
			if(0==status)
			{
				writes++;
				String filename = filenames[rnd.nextInt(filenames.length)];
				try
				{
					FileWriter	fw 				= new FileWriter(directory+filename);
					String str					= new BigInteger(130,new SecureRandom()).toString(32);
					str							= str.concat(" FROM ");
					str							= str.concat( CURRENT_NODE_IP);
					str							= str.concat("\n");
					BufferedWriter bufferWritter= new BufferedWriter(fw);
					bufferWritter.write(str);
					bufferWritter.close();
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
			
			WorkerThread jobThread	= new WorkerThread(command,status,(long)0);
			jobs.add(jobThread);
		}

		System.out.println("Running 100 jobs ...");
		try
		{
			for(int i=0;i<jobs.size();i++)
				jobs.get(i).start();

			for(int i=0;i<jobs.size();i++)
				jobs.get(i).join();
		}
		catch(InterruptedException e){}
	}
		
	static class WorkerThread extends Thread
	{
		public String command;
		public int Optype;
		public long duration;    
	
		public WorkerThread(String s,int Optype,long duration){
        	this.command	= s;
			this.Optype		= Optype;
			this.duration 	= duration;
    	}

		public void run()
		{
			try
			{
				Runtime r 		= Runtime.getRuntime();
        	    this.duration  	= System.currentTimeMillis();
        	    Process p 		= r.exec(this.command); //Executing the command
         	  	this.duration   = System.currentTimeMillis() - this.duration;
			}
			catch(IOException e) {}
		}
	}	
}
