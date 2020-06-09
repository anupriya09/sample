package com;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/*import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;*/
import org.bson.Document;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
/**
 * This Class loads the feature data into Ignite database for both XGBoost
 * and LSTM model inference. 
 */
public class XGBLSTMLoaderMain {
	/**
	 * Hash-map to store user record, read from CSV, while building the features for that user. 
	 */
	static Map<String,LinkedList> recordMap;
	/**
	 * Path to the required data/meta-files files. 
	 */
	final static String filePath = "/F:/iPrescribe/";
	/**
	 * An instance of the class "PreProcessCSV", which processed read "metafile1"  
	 */
	static PreProcessCSV metafile1 = new PreProcessCSV(filePath+"PAKDD_Metafile1.csv");
	/**
	 * An instance of the class "PreProcessMetafile2v2", which processed read "metafile2"  
	 */
	static PreProcessMetafile2v2 metafile2= new PreProcessMetafile2v2(filePath+"PAKDD_Metafile2.csv");
	/**
	 * An instance of the class "FeatureFuncV2".  
	 */
    static FeatureFuncV2 featfunc = new FeatureFuncV2();
    /**
	 * File which contains feature vector, used to train the model in python.    
	 */
	static FileWriter trainingfile ;
	/**
	 * windowsConstant is a list of possible durations in days for window function.    
	 */
    static List<Integer> windowsConstant = Arrays.asList(3,7,14,30,60,90,120,150,180,210,240,270,300,330,360);
    /**
	 * windowsConstant is a list of actual required durations in days for window function.    
	 */
    static List<Integer> windows = new ArrayList<>();
    /**
	 * days is the window duration mentioned by the user via metafile1.     
	 */
    static int days;
    /**
	 * An instance of the class "DictionaryFunc".     
	 */
    static DictionaryFunc dict = new DictionaryFunc(); 
    /**
	 * An instance of the class "DateTimeFormatter" used to convert time into time-format as mentioned in metafile1.     
	 */
    static  DateTimeFormatter dfInCDF = DateTimeFormat.forPattern(metafile1.getCellValueForParticularRow("server_time", "Information"));
    /**
	 * An instance of the class "DateTimeFormatter" used to convert date into date-format as mentioned in metafile1.     
	 */
    static  DateTimeFormatter dft = DateTimeFormat.forPattern("yyyy-MM-dd");
    /**
	 * Integer list having of threshold in days.     
	 */
    public static int[] atleastThresolds = {2, 5, 10, 16, 35};
    
    
 //********************************************** IGNITE CONFIG DECLARATION ************************************************	
    /**
	 * This Field is to explore Ignite instance across nodes.
	 */
  	static TcpDiscoverySpi spi;
  	/**
	 * This Field is to explore Ignite instance across nodes.
	 */
  	static TcpDiscoveryVmIpFinder ipFinder;
  	/**
	 * This Field takes stream data from kafka or to insert data to Ignite
	 * Cache. This is for inserting feature data for XGBoost to Ignite cache.
	 */
  	static IgniteDataStreamer<String, String> 	XGBStreamer;
  	/**
	 * This Field takes stream data from kafka or to insert data to Ignite
	 * Cache. This is for inserting feature data for LSTM to Ignite cache.
	 */
  	static IgniteDataStreamer<String, String> 	LSTMStreamer;
  	/**
	 * This Field is for initializing Ignite node configuration.
	 */
  	public static IgniteConfiguration igniteConfiguration;
  	/**
	 * This Field creates instance of Ignite.
	 */
  	static Ignite ignite;
  	/**
	 * This Field is to declare Ignite cache configuration object. This is to
	 * declare XGBoost cache configuration.
	 */
	static CacheConfiguration<String, String> XGBCfg ;
	/**
	 * This Field is to declare Ignite cache object. This is to declare XGBoost
	 * feature cache.
	 */
	static IgniteCache<String,String> XGBCache;
	/**
	 * This Field is to declare Ignite cache configuration object. This is to
	 * declare LSTM cache configuration.
	 */
	static CacheConfiguration<String, String> LSTMCfg ;
	/**
	 * This Field is to declare Ignite cache object. This is to declare LSTM
	 * feature cache.
	 */
	static IgniteCache<String,String> LSTMCache;
	/**
	 * List of IPs of all nodes, those are supposed to be explored to form
	 * Ignite cluster.
	 */
	static ArrayList listIgniteIp = new ArrayList(Arrays.asList("172.24.24.67:47500..47516","192.168.140.48:47500..47516","192.168.140.49:47500..47516"));
//**************************************************************************************************************************		
	
public static void main(String[] args) throws IOException, InterruptedException {
	
	//trainingfile = new FileWriter("/home/sanket/workspace_python/iPrescribeGeneralization/trainingSetFilePakdd_Sanket.json");
	//trainingfile = new FileWriter("/home/sanket/workspace_python/iPrescribeGeneralization/trainingSetFileInsta_Sanket.json");
    
    days = Integer.parseInt(metafile1.getCellValueForParticularRow("windows", "Information"));
    
    /**
	 * adding days in window list lesser than "days".
	 */
    for (int addWindow : windowsConstant)
        if (addWindow <= days)
            windows.add(addWindow);   
   
  
    ExecutorService executors =  Executors.newFixedThreadPool(60);
    long start_total=0L;
    /**
	 * Defining all Ignite objects.
	 */
	spi = new TcpDiscoverySpi();
	ipFinder = new TcpDiscoveryVmIpFinder();
	/**
	 * List of IPs of all nodes, those are supposed to be explored to form
	 * Ignite cluster.
	 */
	ipFinder.setAddresses(listIgniteIp);
	spi.setIpFinder(ipFinder);
	
		igniteConfiguration = new IgniteConfiguration();
		igniteConfiguration.setPeerClassLoadingEnabled(true);
		//igniteConfiguration.setClientMode(true);
		igniteConfiguration.setSystemThreadPoolSize(60);
		igniteConfiguration.setDiscoverySpi(spi);
	
	ignite = Ignition.start(igniteConfiguration);
	XGBCfg = new CacheConfiguration<>("XGBCache");
	XGBCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
	XGBCache = ignite.getOrCreateCache(XGBCfg);
	
	XGBStreamer= ignite.dataStreamer(XGBCache.getName());
	XGBStreamer.autoFlushFrequency(100);
	XGBStreamer.allowOverwrite(true);
	
	start_total=System.currentTimeMillis();
	/**
	 * Calling API0 for XGBoost.
	 */
	callAPI0(executors);
	
	System.out.println("Time for API0 ms="+ (System.currentTimeMillis()-start_total));
	System.out.println("Starting LSTM");
	LSTMCfg = new CacheConfiguration<>("LSTM");
	LSTMCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
	LSTMCache = ignite.getOrCreateCache(LSTMCfg);
	
	LSTMStreamer= ignite.dataStreamer(LSTMCache.getName());
	LSTMStreamer.autoFlushFrequency(100);
	LSTMStreamer.allowOverwrite(true);
	System.out.println("printing");
	String user;
	Document d=new Document();
	/**
	 * Read the text file containing hidden state of user for LSTM. 
	 */
	BufferedReader br=new BufferedReader(new FileReader(filePath+"hsd.txt"));
	/**
	 * Load LSTM hidden states into Ignite cache.
	 */
	while((user=br.readLine())!=null)
	{
		Document hs=new Document().parse(user);
		Set<String> us=hs.keySet();
		for(String u:us) {
			LSTMCache.put(u,((Document)hs.get(u)).toJson());
		}
		
//		XgboostCache.put("af8fb45",new Document().parse(user));
	}
	System.out.println("Done");
	
    //trainingfile.close();
	}
//***********************************************************************************************************************	

/**
 * This method reads the CDF file and put the user records into hash-map.
 * Further it calls another method called "recordStartThreads" after reading
 * 100K(or end of file) user lines from CDF. After one batch of 100k records, 
 * the hash-map gets cleared and loaded with next batch.  
 *
 * @param executors
 *            Thread pool for multi-threading.
 * @throws IOException 
 *            File input/output exception, 
 * @throws InterruptedException
 * 				If thread is interrupted during the activity.
 */
private static void callAPI0(ExecutorService executors) throws IOException, InterruptedException {
	 recordMap = new ConcurrentHashMap<String,LinkedList>(100000,0.75f,100);
	 //Reader reader = new FileReader("/home/sanket/workspace_python/iPrescribeGeneralization/sorted_full_supercsv_final.csv");
	 Reader reader = new FileReader(filePath+"sorted_full_filled_pakdd_c636d37_5302e20.csv");
	 
	 try {
		 List<String> Header= CSV.parseLine(reader); //getting header from CSV file.  **can be changed
		 //StructType schema = new StructType();
	
	 LinkedList<Document> recordList;
	 String key="";
	 int count=0;
	 long start=0L;
	 List<String> list;
		while((list=CSV.parseLine(reader))!=null)
		 {
                Document doc = new Document();
		        for (int i=0; i<Header.size(); i++) {
		            doc.put(Header.get(i), list.get(i));
		            
		          }
		     //System.out.println(doc.toJson());
			 key=(String)doc.getString("uid");
			if(recordMap.containsKey(key))
			{
				recordMap.get(key).add(doc);
			}
			else
			{
				recordList = new LinkedList<Document>();
				recordList.add(doc);
				recordMap.put(key, recordList);
			}
			count++;
			if(count%100000==0)
			{
				start=System.currentTimeMillis();
				recordStartThreads(executors);
				recordMap.clear();
				System.out.println("Time for this batch="+(System.currentTimeMillis()-start));
			}
		 }
	
		if(recordMap.size()!=0)
		{
		 	start=System.currentTimeMillis();
			recordStartThreads(executors);
			System.out.println("Check record "+count+" "+recordMap.size());
			recordMap.clear();
			System.out.println("Time for this batch="+(System.currentTimeMillis()-start));
		}
	 } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
}

/**
 * This method iterate over the user record Hash-map and spawn one thread for each user.
 * Each spawned thread executes the instance of class "API0Runnable" which implements runnable
 * inteface for API0 processing.   
 *
 * @param executors
 *            Thread pool for multi-threading.
 * @throws InterruptedException
 *  		If thread is interrupted during the activity.
 */

private static void recordStartThreads(ExecutorService executors) throws InterruptedException {
	int size = recordMap.keySet().size();
//	System.out.println("*****************Size "+recordMap.size());
//	System.out.println("master id--"+Thread.currentThread().getId());
	int i=0;
	/**
	 * Instance of "CompletionService" to check whether spawned thread has completed
	 * their execution so that main thread will resume. 
	 */
	CompletionService<Boolean> completion = new ExecutorCompletionService(executors);
	
	for(String uid:recordMap.keySet())
	 {
		i++;
		//System.out.println("UID----------------- "+uid);
		Thread thread = new Thread(new API0Runnable(uid));
		Boolean result=false;
		//executors.submit(thread);
		completion.submit(thread, result);	
	}
	
	for (i = 0; i < size; ++i) {
	     completion.take(); // will block until the next sub task has completed.
	}
	//System.out.println("Check2 ");	
}
 
}
