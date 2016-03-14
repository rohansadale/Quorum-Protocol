import java.util.*;
import java.io.*;

public class Util
{
	private static int MOD = 107;
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
}
