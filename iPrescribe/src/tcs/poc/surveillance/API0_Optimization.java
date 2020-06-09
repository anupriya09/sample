package tcs.poc.surveillance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class API0_Optimization {

	//static Map<String,LinkedList> orderMap;
	static HashMap<String, HashMap<String, Integer>> userProdMap;
	static HashMap<String, Double> buyingProbMap;
	static HashMap<String, Double> priceProdMap;
	
	//********************************************** IGNITE CONFIG DECLARATION ************************************************	
	static TcpDiscoverySpi spi;
	static TcpDiscoveryVmIpFinder ipFinder;
	static IgniteDataStreamer<String, String> 	buyingProbStreamer;
	static IgniteDataStreamer<String, Double> 	priceProdStreamer;
	/*{
		ipFinder.setAddresses(
				Arrays.asList("192.168.140.44:47500..47516"));//,"192.168.140.48:47500..47516"));//,"192.168.140.49:47500..47516")); 
				//Arrays.asList("192.168.140.44:47500..47509","192.168.140.48:47500..47509"));
		spi.setIpFinder(ipFinder);
		}*/
	//public static IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
	public static IgniteConfiguration igniteConfiguration;
	/*{
		//igniteConfiguration.setPeerClassLoadingEnabled(true);
		igniteConfiguration.setClientMode(true);
		//igniteConfiguration.setSystemThreadPoolSize(60000);
		igniteConfiguration.setDiscoverySpi(spi);
	}*/
	static Ignite ignite;
	static CacheConfiguration<String, String> buyingProbCfg ;
	static CacheConfiguration<String, Double> priceProdCfg ;
	/*{
		UserDataCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
	}*/
	static IgniteCache<String,String> buyingProbCache;
	static IgniteCache<String,Double> priceProdCache;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		BufferedReader brOrder = new BufferedReader(new FileReader("F:/iPrescribe/OrderProductmnsfinal.csv"));
		ExecutorService executors =  Executors.newFixedThreadPool(60);
		
		long start_total=0L;
		spi = new TcpDiscoverySpi();
		ipFinder = new TcpDiscoveryVmIpFinder();
		ipFinder.setAddresses(
				Arrays.asList("172.24.24.67:47500..47516"));//,"192.168.140.48:47500..47516"));//,"192.168.140.49:47500..47516")); 
				//Arrays.asList("192.168.140.44:47500..47509","192.168.140.48:47500..47509"));
		spi.setIpFinder(ipFinder);
		
		igniteConfiguration = new IgniteConfiguration();
		//igniteConfiguration.setPeerClassLoadingEnabled(true);
		//igniteConfiguration.setClientMode(true);
		igniteConfiguration.setSystemThreadPoolSize(60);
		igniteConfiguration.setDiscoverySpi(spi);
		
		ignite = Ignition.start(igniteConfiguration);
		
		buyingProbCfg = new CacheConfiguration<>("BuyingProb");
		buyingProbCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		buyingProbCache = ignite.getOrCreateCache(buyingProbCfg);
		
		buyingProbStreamer= ignite.dataStreamer(buyingProbCache.getName());
		buyingProbStreamer.autoFlushFrequency(100);
		buyingProbStreamer.allowOverwrite(true);
		
		priceProdCfg = new CacheConfiguration<>("PriceProd");
		priceProdCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		priceProdCache = ignite.getOrCreateCache(priceProdCfg);
		
		priceProdStreamer= ignite.dataStreamer(priceProdCache.getName());
		priceProdStreamer.autoFlushFrequency(100);
		priceProdStreamer.allowOverwrite(true);
		
		start_total=System.currentTimeMillis();
		findBuyingProbability(brOrder,executors);
		brOrder.close();
		System.out.println("Time for Order ms="+ (System.currentTimeMillis()-start_total));
		
	}
		
		//********************************************** API 0 Order *************************************************************
		public static void findBuyingProbability(BufferedReader brOrder, ExecutorService executors) throws IOException, InterruptedException {
						 
			 userProdMap = new HashMap<>();
			 priceProdMap = new HashMap<String, Double>();
			
			 String msg="";
			 String key="";
			 String prodId="";
			 double price;
			 int quantity;
			 String s[]=null;
			 int count=0;
			 long start=0L;
			 brOrder.readLine();
			 
			 while((msg=brOrder.readLine())!=null && count<30)
			 {
				s = msg.split(",");
				/*msgDoc = new Document();
				msgDoc.append("server_time", s[0]);
				msgDoc.append("device", s[1]);
				msgDoc.append("session_id", s[2]);
				msgDoc.append("uid", s[3]);
				msgDoc.append("order_id", s[4]);
				msgDoc.append("quantity", s[5]);
				msgDoc.append("price", s[6]);
				msgDoc.append("category1", s[7]);
				msgDoc.append("category2", s[8]);
				msgDoc.append("category3", s[9]);
				msgDoc.append("category4", s[10]);
				msgDoc.append("brand", s[11]);
				msgDoc.append("item_id", s[12]);
				System.out.println(msgDoc);
				key = msgDoc.getString("uid");*/
				
			//	System.out.println("Printing s: "+s[3]+"  "+s[12]+"  "+s[6]+"  "+s[5]);
				if(s[3]!=null && s[12]!=null && s[6]!= null && s[5]!=null && !s[3].isEmpty() && !s[12].isEmpty() && !s[6].isEmpty() && !s[5].isEmpty()){
				
				key = s[3];
				prodId = s[7];
				price = Double.parseDouble(s[6]);
				quantity = Integer.parseInt(s[5]);
				//System.out.println("Uid: "+key);
				
				if(userProdMap.containsKey(key))
				{
					HashMap<String, Integer> prodHashMap= userProdMap.get(key);
						if(prodHashMap.containsKey(prodId)){
							prodHashMap.put(prodId, prodHashMap.get(prodId)+1);
						}
						else {
							prodHashMap.put(prodId, 1);
						}
					userProdMap.put(key, prodHashMap);
				}
				else
				{
					//ArrayList<HashMap<String, Integer>> prodList = new ArrayList<>();
					HashMap<String, Integer> prodHashMap = new HashMap<String, Integer>();
					prodHashMap.put(prodId, 1);
					//prodList.add(prodHashMap);
					userProdMap.put(key, prodHashMap);
				}
				
				if(!priceProdMap.containsKey(prodId)){
					priceProdMap.put(prodId, price/quantity);
				}
				
				}
				count++;
				
			/*	if(count%2000000==0)
				{
					start=System.currentTimeMillis();
					orderStartThreads(executors);
					//viewMap = new ConcurrentHashMap<String,LinkedList>();
					//System.out.println("Check "+count+" "+orderMap.size());
					userProdMap.clear();
					System.out.println("Time for this batch="+(System.currentTimeMillis()-start));
				}*/
			 }
			 
			 System.out.println("Done with buiding map for all users");
			 
				if(userProdMap.size()!=0)
				{
				 	start=System.currentTimeMillis();
					orderStartThreads(executors);
					//viewMap = new ConcurrentHashMap<String,LinkedList>();
					System.out.println("Check order "+count+" "+userProdMap.size());
					userProdMap.clear();
					System.out.println("Time for this batch="+(System.currentTimeMillis()-start));
				}
			  
				BufferedWriter bw = new BufferedWriter(new FileWriter("F:/iPrescribe/PriceCategory1.csv"));
				
				for (Entry<String, Double> entry : priceProdMap.entrySet()){
					//priceProdStreamer.addData(entry.getKey(), entry.getValue());
					bw.write(entry.getKey()+","+entry.getValue());bw.write("\n");
				}
				bw.close();				
		}
		
		private static void orderStartThreads(ExecutorService executors) throws InterruptedException {
			int size = userProdMap.keySet().size();
			int i=0;
			CompletionService<Boolean> completion = new ExecutorCompletionService<Boolean>(executors);
			for(String uid:userProdMap.keySet())
			 {
				i++;
				Thread thread = new Thread(new OptimizationRunnable(uid));
				Boolean result=false;
				//executors.submit(thread);
				completion.submit(thread, result);	
			}
			for (i = 0; i < size; ++i) {
			     completion.take(); // will block until the next sub task has completed.
			}
			System.out.println("Check2 ");	
		}
}
