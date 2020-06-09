package tcs.poc.surveillance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamVisitor;
import org.bson.Document;

/**
 * This Class creates XGBOOST feature dictionary i.e. API0.
 */

public class PAKDDLSTMCDFHiddenStatesAPI0_Ensemble {
	public static Map<String, LinkedList> cdfMap;
	public static Map<String, ArrayList<String>> headFuncDict;
	public static Map<String, String> typedict;
	public static BufferedReader configread;
	public static ArrayList<String> funcType;
	public static ArrayList<String> catHeaderList;
	public static Set<String> users = new HashSet<String>();
	// static ArrayList<String> ytrain=new ArrayList<String>();

	/**
	 * This Field viewMap contains view data from csv.
	 */
	public static Map<String, LinkedList> viewMap;
	/**
	 * This Field orderMap contains Order data from csv.
	 */
	public static Map<String, LinkedList> orderMap;
	/**
	 * This Field impMap contains impression data from csv.
	 */
	public static Map<String, LinkedList> impMap;
	
	public static ArrayList<Long> windows = new ArrayList<>(Arrays.asList(3L, 7L, 14L, 30L, 45L));
	public static int[] atleastOrderThresholds = { 2, 5, 7, 10 };
	public static ArrayList<String> highCardColumns = new ArrayList<String>(
			Arrays.asList("order_Count_Category2", "order_Count_Category3", "order_Count_Category4"));
	// ********************************************** IGNITE CONFIG DECLARATION
	// ************************************************
	/**
	 * This Field is to explore Ignite instance across nodes.
	 */
	public static TcpDiscoverySpi spi;
	/**
	 * This Field is to explore Ignite instance across nodes.
	 */
	public static TcpDiscoveryVmIpFinder ipFinder;
	/**
	 * This Field takes stream data from kafka or to insert data to Ignite
	 * Cache. This is for inserting LSTM sequence to Ignite cache.
	 */
	public static IgniteDataStreamer<String, String> LSTMDictStreamer;
	/**
	 * This Field takes stream data from kafka or to insert data to Ignite
	 * Cache. This is for inserting XGBOOST features to Ignite cache.
	 */
	public static IgniteDataStreamer<String, String> XGBOOSTStreamer;
	/**
	 * This Field is for initializing Ignite node configuration.
	 */
	public static IgniteConfiguration igniteConfiguration;
	/**
	 * This Field creates instance of Ignite.
	 */
	public static Ignite ignite;
	
	public static CacheConfiguration<String, String> LSTMDictCfg;
	public static CacheConfiguration<String, String> LSTMCfg;
	/**
	 * This Field is to declare Ignite cache configuration object. This is to
	 * declare XGBOOST cache configuration.
	 */
	public static CacheConfiguration<String, String> XGBOOSTCfg;
	/*
	 * { UserDataCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED); }
	 */
	public static IgniteCache<String, String> LSTMDictCache;
	public static IgniteCache<String, String> LSTMCache;
	/**
	 * This Field is to declare Ignite cache object. This is to
	 * declare XGBOOSTCache cache configuration.
	 */
	public static IgniteCache<String, String> XGBOOSTCache;

	// *************************************************************************************************************************

	public static String toCodesDirect2(String value) {
		Character[] fullsetarr = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
				'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
				'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6',
				'7', '8', '9', '.' };
		ArrayList<Character> fullset = new ArrayList<Character>(Arrays.asList(fullsetarr));
		String code = "";
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (Character.isDigit(c)) {
				code += c;
			} else {
				int ind = fullset.indexOf(c) + 10;
				code += Integer.toString(ind);
			}

		}
		if (code.equals("")) {
			return "0.0";
		}
		return code;
	}

	public static Document transactionType(Document lstmData, String colLev0Value, String colLev1Value,
			String columnValue, String header, int eventNumber) {
		String putvalue = "";
		if (columnValue.equals("impression")) {
			putvalue = "1.0";
		} else {
			putvalue = "0.0";
		}
		Set<String> users = lstmData.keySet();
		String functionName = "direct";
		ArrayList<String> features = new ArrayList<>();
		features.add(putvalue);
		if (!users.contains(colLev0Value)) {
			Document user = new Document();
			user.put("isImpression", features);
			lstmData.put(colLev0Value, user);

		} else {
			Document user = (Document) lstmData.get(colLev0Value);
			if (!user.containsKey("isImpression")) {
				user.put("isImpression", features);
			} else {
				ArrayList<String> alreadyfeatures = (ArrayList<String>) user.get("isImpression");

				alreadyfeatures.add(putvalue);
				user.replace("isImpression", alreadyfeatures);
				lstmData.replace(colLev0Value, user);
			}

		}
		return lstmData;
	}

	public static Document directfromDF(Document lstmData, String colLev0Value, String colLev1Value, String columnValue,
			String header, int eventNumber, String fillerValue, ArrayList<String> catHeaderList, boolean istrain,
			Map<String, String> typelist) {
		Set<String> users = lstmData.keySet();
		String functionName = "direct";
		if (columnValue.length() == 0) {
			if (typelist.get(header).equals("continuous")) {
				columnValue = "0";
			}
		}
		if (catHeaderList.size() > 0 && (catHeaderList.contains(header))) {
			columnValue = toCodesDirect2(columnValue);
		}
		ArrayList<String> features = new ArrayList<>();
		features.add(columnValue);
		if (!users.contains(colLev0Value)) {
			Document user = new Document();
			user.put(header + '_' + functionName, features);
			lstmData.put(colLev0Value, user);

		} else {
			Document user = (Document) lstmData.get(colLev0Value);
			if (!user.containsKey(header + '_' + functionName)) {
				user.put(header + '_' + functionName, features);
			} else {
				ArrayList<String> alreadyfeatures = (ArrayList<String>) user.get(header + '_' + functionName);
				alreadyfeatures.add(columnValue);
				user.replace(header + '_' + functionName, alreadyfeatures);
				lstmData.replace(colLev0Value, user);
			}

		}
		return lstmData;
	}

	/*
	 * public static void createCdfDict(BufferedReader cdfread,BufferedReader
	 * configread)throws IOException { Document lstmData=new Document(); cdfMap
	 * = new ConcurrentHashMap<String,LinkedList>(100000,0.75f,100);
	 * //LinkedList<Document> viewList; LinkedList<String> cdfList;
	 * //LinkedList<Document> listDoc; String msg=""; String key=""; String
	 * s[]=null; int count=0; while((msg=cdfread.readLine())!=null)// &&
	 * count<1000000) { s = msg.split(","); key=s[3];
	 * if(cdfMap.containsKey(key)) { cdfMap.get(key).add(msg); } else { cdfList
	 * = new LinkedList<String>(); cdfList.add(msg); cdfMap.put(key, cdfList); }
	 * }
	 * 
	 * funcType = new ArrayList<String>(); funcType.add("TypeH");
	 * funcType.add("TypeC"); funcType.add("TypeE"); funcType.add("TypeG");
	 * funcType.add("TypeD"); funcType.add("TypeK"); funcType.add("TypeL");
	 * funcType.add("TypeM"); funcType.add("TypeI"); funcType.add("TypeJ");
	 * funcType.add("TypeN"); funcType.add("TypeO"); String columnLevels[]=
	 * {"uid"};
	 * 
	 * Object[] confdicts=configDicts(configread,funcType);
	 * Map<String,ArrayList<String>>
	 * headFuncDict=(Map<String,ArrayList<String>>)confdicts[0];
	 * Map<String,String> typedict=(Map<String,String>)confdicts[1];
	 * 
	 * 
	 * if(columnLevels.length==2) {
	 * 
	 * }else { String cdfColLev1 = ""; catHeaderList=new ArrayList<String>();
	 * for (Map.Entry<String,ArrayList<String>> entry : headFuncDict.entrySet())
	 * {
	 * 
	 * ArrayList<String> functionList=entry.getValue(); for(String
	 * func:functionList) { if(func.equals("TypeI")){
	 * catHeaderList.add(entry.getKey()); } ////System.out.println("Key = " +
	 * entry.getKey() + ", Value = " + entry.getValue()); } }
	 * 
	 * 
	 * 
	 * 
	 * /////////////////////////////////////////////////////////////////////////
	 * /////////////////////////////////////
	 * 
	 * 
	 * 
	 * for(String uid:cdfMap.keySet()){ LinkedList<String> listDoc =
	 * cdfMap.get(uid); Iterator iter = listDoc.iterator(); Document record;
	 * String row=""; String currentLevel0=uid; while(iter.hasNext()) { row =
	 * (String)iter.next(); s = row.split(",",22); record = new Document(); int
	 * eventNumber = 0; int userChangeFlag = 0; //System.out.println("entered");
	 * record.append("server_time", s[0]); record.append("device", s[1]);
	 * record.append("session_id", s[2]); record.append("uid", s[3]);
	 * record.append("transaction_id", s[4]); record.append("quantity", s[5]);
	 * record.append("price", s[6]); record.append("category1", s[7]);
	 * record.append("category2", s[8]); record.append("category3", s[9]);
	 * record.append("category4", s[10]); record.append("brand", s[11]);
	 * record.append("item_id", s[12]); record.append("transaction_type",
	 * s[13]); record.append("app_code", s[14]); record.append("inventory_type",
	 * s[15]); record.append("is_click", s[16]); record.append("is_conversion",
	 * s[17]); record.append("model", s[18]); record.append("network", s[19]);
	 * record.append("os_version", s[20]); record.append("platform", s[21]);
	 * 
	 * for (Map.Entry<String,ArrayList<String>> entry : headFuncDict.entrySet())
	 * { //System.out.println("entered2"); ArrayList<String>
	 * functionList=entry.getValue(); for(String func:functionList) {
	 * //System.out.println("entered3"); String fillerValue="";
	 * if(func.equals("TypeN")){ lstmData=directfromDF(lstmData, currentLevel0,
	 * cdfColLev1, record.get(entry.getKey()).toString(), entry.getKey(),
	 * eventNumber, fillerValue,catHeaderList,true,typedict); }
	 * if(func.equals("TypeO")){ ////System.out.println()
	 * lstmData=transactionType(lstmData, currentLevel0,
	 * cdfColLev1,record.get(entry.getKey()).toString(), entry.getKey(),
	 * eventNumber); } ////System.out.println("Key = " + entry.getKey() +
	 * ", Value = " + entry.getValue()); } }//System.out.println(lstmData);
	 * 
	 * } } } //System.out.println(lstmData);
	 * 
	 * }
	 */
	public static void createCdfDict2(BufferedReader cdfread, BufferedReader configread, ExecutorService executors)
			throws IOException, InterruptedException {

		cdfMap = new ConcurrentHashMap<String, LinkedList>(100000, 0.75f, 100);
		// LinkedList<Document> viewList;
		LinkedList<String> cdfList;
		// LinkedList<Document> listDoc;
		String msg = "";
		String key = "";
		String s[] = null;
		int count = 0;
		ArrayList<String> funcType = new ArrayList<String>();
		funcType.add("TypeH");
		funcType.add("TypeC");
		funcType.add("TypeE");
		funcType.add("TypeG");
		funcType.add("TypeD");
		funcType.add("TypeK");
		funcType.add("TypeL");
		funcType.add("TypeM");
		funcType.add("TypeI");
		funcType.add("TypeJ");
		funcType.add("TypeN");
		funcType.add("TypeO")	;
		Object[] confdicts = configDicts(configread, funcType);
		headFuncDict = (Map<String, ArrayList<String>>) confdicts[0];
		typedict = (Map<String, String>) confdicts[1];
		String header=cdfread.readLine();
		System.out.println("header.."+header);
		while ((msg = cdfread.readLine()) != null)// && count<1000000)
		{
			s = msg.split(",");
			//System.out.println("Size of s.."+ s.length);
			key = s[24];
			
			if (cdfMap.containsKey(key)) {
				cdfMap.get(key).add(msg);
			} else {
				cdfList = new LinkedList<String>();
				cdfList.add(msg);
				cdfMap.put(key, cdfList);
			}

			String columnLevels[] = { "uid" };

			if (columnLevels.length == 2) {

			} else {
				String cdfColLev1 = "";
				catHeaderList = new ArrayList<String>();
				for (Map.Entry<String, ArrayList<String>> entry : headFuncDict.entrySet()) {

					ArrayList<String> functionList = entry.getValue();
					for (String func : functionList) {
						if (func.equals("TypeI")) {
							catHeaderList.add(entry.getKey());
						}
						//// System.out.println("Key = " + entry.getKey() + ",
						//// Value = " + entry.getValue());
					}
				}

				count++;
				if (count % 2000000 == 0) {
					// start=System.currentTimeMillis();
					cdfStartThreads(executors);
					// viewMap = new ConcurrentHashMap<String,LinkedList>();
					System.out.println("Check " + count + " " + cdfMap.size());
					cdfMap.clear();
					//// System.out.println("Time for this
					//// batch="+(System.currentTimeMillis()-start));
				}

				//////////////////////////////////////////////////////////////////////////////////////////////////////////////

			}

		}
		if (cdfMap.size() != 0) {
			// start=System.currentTimeMillis();
			cdfStartThreads(executors);
			// viewMap = new ConcurrentHashMap<String,LinkedList>();
			System.out.println("Check " + count + " " + cdfMap.size());
			cdfMap.clear();
			//// System.out.println("Time for this
			//// batch="+(System.currentTimeMillis()-start));
		}

		/*
		 * for(String uid:viewMap.keySet()) { count_T++;
		 * //System.out.println("Count_T="+count_T); Thread thread = new
		 * Thread(new ViewRunnable(uid)); executors.submit(thread);
		 * thread.join(); }
		 */
		// cdfread.close();
		configread.close();
		// Thread.currentThread().join();
	}

	public static void cdfStartThreads(ExecutorService executors) throws InterruptedException {
		int size = cdfMap.keySet().size();
		int i = 0;
		CompletionService<Boolean> completion = new ExecutorCompletionService(executors);
		for (String uid : cdfMap.keySet()) {
			i++;

			Thread thread = new Thread(new CDFRunnable2(uid));
			Boolean result = false;
			// executors.submit(thread);
			completion.submit(thread, result);
		}
		for (i = 0; i < size; ++i) {
			completion.take(); // will block until the next sub task has
								// completed.
		}
		// System.out.println("Check2 ");
	}

	public static Object[] configDicts(BufferedReader config, ArrayList<String> availLSTMFunctionTypes)
			throws IOException {
		Map<String, ArrayList<String>> functionDict = new HashMap<String, ArrayList<String>>();
		Map<String, String> typeDict = new HashMap<String, String>();
		String row;

		while ((row = config.readLine()) != null) {
			// System.out.println("row");
			// System.out.print(row);
			String[] elements = row.split(",", 6);
			// System.out.println("\nelements");
			// System.out.println(Arrays.asList(elements));
			String functions = elements[3];
			// System.out.println("functions:"+functions);
			if (functions.length() > 0) {
				String[] functionarray = functions.split(";");
				ArrayList<String> functionList = new ArrayList<>(Arrays.asList(functionarray));
				for (String func : functionList) {
					if (availLSTMFunctionTypes.contains(func)) {
						ArrayList<String> l = new ArrayList<String>(functionDict.keySet());
						String header = elements[1];
						if (l.contains(header)) {
							ArrayList<String> funcs = functionDict.get(header);
							funcs.add(func);
							functionDict.put(header, funcs);
						} else {
							ArrayList<String> funcs = new ArrayList<String>();
							funcs.add(func);
							functionDict.put(header, funcs);
						}
					}
				}

			}
			typeDict.put(elements[1], elements[2]);

		}
		// System.out.println("functionDict");
		// System.out.println(functionDict);
		return new Object[] { functionDict, typeDict };
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		// BufferedReader cdfread = new BufferedReader(new
		// FileReader("/home/sharod/AllCodes/iRetailLSTMCDF/Abstraction/Datasets/PAKDDCDF.csv"));

		BufferedReader cdfread = new BufferedReader(
				new FileReader("F:/iPrescribe/sorted_full_filled_pakdd_mnsfinal.csv"));

		// configread = new BufferedReader(new
		// FileReader("/home/sharod/AllCodes/iRetailLSTMCDF/Abstraction/Datasets/PAKDDConfigNew2.csv"));
		configread = new BufferedReader(
				new FileReader("F:/iPrescribe/PAKDDConfigNew2.csv"));

		System.out.println("File read...");
		ExecutorService executors = Executors.newFixedThreadPool(60);
		long start_total = 0L;
		spi = new TcpDiscoverySpi();
		ipFinder = new TcpDiscoveryVmIpFinder();
		ipFinder.setAddresses(
				Arrays.asList("172.24.24.67:47500..47516"));// ,"192.168.140.48:47500..47516"));//,"192.168.140.49:47500..47516"));
		// Arrays.asList("192.168.140.44:47500..47509","192.168.140.48:47500..47509"));
		spi.setIpFinder(ipFinder);

		igniteConfiguration = new IgniteConfiguration();
		//igniteConfiguration.setPeerClassLoadingEnabled(true);
		//igniteConfiguration.setClientMode(true);
		 igniteConfiguration.setSystemThreadPoolSize(60);
		igniteConfiguration.setDiscoverySpi(spi);

		ignite = Ignition.start(igniteConfiguration);
		LSTMDictCfg = new CacheConfiguration<>("LSTMDict");
		LSTMDictCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		LSTMDictCache = ignite.getOrCreateCache(LSTMDictCfg);

		LSTMCfg = new CacheConfiguration<>("LSTM");
		LSTMCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		LSTMCache = ignite.getOrCreateCache(LSTMCfg);

		XGBOOSTCfg = new CacheConfiguration<>("XGBOOST");
		XGBOOSTCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		XGBOOSTCache = ignite.getOrCreateCache(XGBOOSTCfg);

		LSTMDictStreamer = ignite.dataStreamer(LSTMDictCache.getName());
		LSTMDictStreamer.autoFlushFrequency(100);
		LSTMDictStreamer.allowOverwrite(true);

		XGBOOSTStreamer = ignite.dataStreamer(XGBOOSTCache.getName());
		XGBOOSTStreamer.autoFlushFrequency(100);
		XGBOOSTStreamer.allowOverwrite(true);

		start_total = System.currentTimeMillis();
		createCdfDict2(cdfread, configread, executors);
		System.out.println("finished cdf");
		System.out.println("Time for CDF ms=" + (System.currentTimeMillis() - start_total));

		start_total = System.currentTimeMillis();
		// createViewFeatureDict(executors);
		// System.out.println("Time for View ms="+
		// (System.currentTimeMillis()-start_total));

		start_total = System.currentTimeMillis();
		// createOrderFeatureDict(executors);
		// System.out.println("Time for Order ms="+
		// (System.currentTimeMillis()-start_total));

		start_total = System.currentTimeMillis();
		// createImpFeatureDict(executors);
		// System.out.println("Time for Imp ms="+
		// (System.currentTimeMillis()-start_total));

		start_total = System.currentTimeMillis();
		// createOneHotVector();
		// System.out.println("Time for HotVector ms="+
		// (System.currentTimeMillis()-start_total));

		// System.out.println(users);
		PrintWriter writer = new PrintWriter("F:/iPrescribe/dictdumpmnsfinal.txt", "UTF-8");
		PrintWriter writer3 = new PrintWriter("F:/iPrescribe/usersdumpmnsfinal.txt", "UTF-8");

		for (String uid : users) {
			System.out.println(uid);
			writer.println(LSTMDictCache.get(uid));
			writer3.println(uid);
			
		}
		writer.close();
		writer3.close();
		System.out.println("writing done...");
		// PrintWriter writer2 = new PrintWriter("ytraindump.txt", "UTF-8");
		// //System.out.println("ytrain"+ytrain);
		// for(String yt:ytrain) {
		// writer2.println(yt);
		//
		// }
		// writer2.close();
		// System.out.println("users"+users);

	}

	// ********************************************** API 0 Imp
	// *************************************************************
	
	
	
	
	/**
	 * This method reads Impression CSV file and add records into impMap with UserId as key.
	 * It calls impStartThreads internally to multi-thread the process of creation of User Dictionary. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  IOException
	 * @throws  InterruptedException
	 */
	private static void createImpFeatureDict(ExecutorService executors) throws IOException, InterruptedException {
		BufferedReader brImp = new BufferedReader(
				new FileReader("/D/mukund/iPrescribe_testing/required_files/PAKDDCDF.csv"));
		impMap = new ConcurrentHashMap<String, LinkedList>(100000, 0.75f, 100);
		LinkedList<String> impList;
		// LinkedList<Document> orderDoc;
		String msg = "";
		String key = "";
		Document msgDoc;
		String s[] = null;
		int count = 0;
		int count_T = 0;
		long start = 0L;
		brImp.readLine();
		while ((msg = brImp.readLine()) != null)// && count<1000000)
		{
			s = msg.split(",");
			if (s[13].equals("impression")) {
				/*
				 * msgDoc = new Document(); msgDoc.append("impression_id",
				 * s[0]); msgDoc.append("server_time", s[1]);
				 * msgDoc.append("uid", s[2]); msgDoc.append("platform", s[3]);
				 * msgDoc.append("inventory_type", s[4]);
				 * msgDoc.append("app_code", s[5]); msgDoc.append("os_version",
				 * s[6]); msgDoc.append("model", s[7]); msgDoc.append("network",
				 * s[8]); msgDoc.append("is_click", s[9]);
				 * msgDoc.append("is_conversion", s[10]);
				 * ////System.out.println(msgDoc); key =
				 * msgDoc.getString("uid");
				 */
				key = s[3];
				if (impMap.containsKey(key)) {
					impMap.get(key).add(msg);
				} else {
					impList = new LinkedList<String>();
					impList.add(msg);
					impMap.put(key, impList);
				}
				count++;
				if (count % 200000 == 0) {
					start = System.currentTimeMillis();
					impStartThreads(executors);
					// viewMap = new ConcurrentHashMap<String,LinkedList>();
					//// System.out.println("Check "+count+" "+impMap.size());
					impMap.clear();
					// System.out.println("Time for this
					// batch="+(System.currentTimeMillis()-start));
				}
			}
		}
		if (orderMap.size() != 0) {
			start = System.currentTimeMillis();
			impStartThreads(executors);
			// viewMap = new ConcurrentHashMap<String,LinkedList>();
			// System.out.println("Check Imp "+count+" "+impMap.size());
			impMap.clear();
			// System.out.println("Time for this
			// batch="+(System.currentTimeMillis()-start));
		}

	}

	/**
	 * This method spawn thread for each user by calling ImpRunnable_Enseble method internally. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  InterruptedException
	 */
	private static void impStartThreads(ExecutorService executors) throws InterruptedException {
		int size = impMap.keySet().size();
		int i = 0;
		CompletionService<Boolean> completion = new ExecutorCompletionService(executors);
		for (String uid : impMap.keySet()) {
			i++;
			Thread thread = new Thread(new ImpRunnable_Enseble(uid));
			Boolean result = false;
			// executors.submit(thread);
			completion.submit(thread, result);
		}
		for (i = 0; i < size; ++i) {
			completion.take(); // will block until the next sub task has
								// completed.
		}
		// System.out.println("Check2 ");
	}

	// ********************************************** API 0 Order
	// *************************************************************
	/**
	 * This method reads Order CSV file and add records into orderMap with UserId as key.
	 * It calls orderStartThreads internally to multi-thread the process of creation of User Dictionary. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  IOException
	 * @throws  InterruptedException
	 */
	public static void createOrderFeatureDict(ExecutorService executors) throws IOException, InterruptedException {
		BufferedReader brOrder = new BufferedReader(
				new FileReader("/D/mukund/iPrescribe_testing/required_files/PAKDDCDF.csv"));

		orderMap = new ConcurrentHashMap<String, LinkedList>(100000, 0.75f, 100);
		LinkedList<String> orderList;
		// LinkedList<Document> orderDoc;
		String msg = "";
		String key = "";
		Document msgDoc;
		String s[] = null;
		int count = 0;
		long start = 0L;
		brOrder.readLine();
		while ((msg = brOrder.readLine()) != null)// && count<10)
		{
			s = msg.split(",");
			if (s[13].equals("order")) {
				/*
				 * msgDoc = new Document(); msgDoc.append("server_time", s[0]);
				 * msgDoc.append("device", s[1]); msgDoc.append("session_id",
				 * s[2]); msgDoc.append("uid", s[3]); msgDoc.append("order_id",
				 * s[4]); msgDoc.append("quantity", s[5]);
				 * msgDoc.append("price", s[6]); msgDoc.append("category1",
				 * s[7]); msgDoc.append("category2", s[8]);
				 * msgDoc.append("category3", s[9]); msgDoc.append("category4",
				 * s[10]); msgDoc.append("brand", s[11]);
				 * msgDoc.append("item_id", s[12]);
				 * //System.out.println(msgDoc); key = msgDoc.getString("uid");
				 */
				key = s[3];
				if (orderMap.containsKey(key)) {
					orderMap.get(key).add(msg);
				} else {
					orderList = new LinkedList<String>();
					orderList.add(msg);
					orderMap.put(key, orderList);
				}
				count++;
				if (count % 200000 == 0) {
					start = System.currentTimeMillis();
					orderStartThreads(executors);
					// viewMap = new ConcurrentHashMap<String,LinkedList>();
					//// System.out.println("Check "+count+" "+orderMap.size());
					orderMap.clear();
					// System.out.println("Time for this
					// batch="+(System.currentTimeMillis()-start));
				}

			}
		}
		if (orderMap.size() != 0) {
			start = System.currentTimeMillis();
			orderStartThreads(executors);
			// viewMap = new ConcurrentHashMap<String,LinkedList>();
			// System.out.println("Check order "+count+" "+orderMap.size());
			orderMap.clear();
			// System.out.println("Time for this
			// batch="+(System.currentTimeMillis()-start));
		}

	}

	/**
	 * This method spawn thread for each user by calling OrderRunnable_Ensemble method internally. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  InterruptedException
	 */
	private static void orderStartThreads(ExecutorService executors) throws InterruptedException {
		int size = orderMap.keySet().size();
		int i = 0;
		CompletionService<Boolean> completion = new ExecutorCompletionService(executors);
		for (String uid : orderMap.keySet()) {
			i++;
			Thread thread = new Thread(new OrderRunnable_Ensemble(uid));
			Boolean result = false;
			// executors.submit(thread);
			completion.submit(thread, result);
		}
		for (i = 0; i < size; ++i) {
			completion.take(); // will block until the next sub task has
								// completed.
		}
		// System.out.println("Check2 ");
	}

	// ********************************************** API 0 VIEW
	// *************************************************************

	/**
	 * This method reads View CSV file and add records into viewMap with UserId as key.
	 * It calls viewStartThreads internally to multi-thread the process of creation of User Dictionary. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  IOException
	 * @throws  InterruptedException
	 */
	public static void createViewFeatureDict(ExecutorService executors) throws IOException, InterruptedException {
		BufferedReader brView = new BufferedReader(
				new FileReader("/D/mukund/iPrescribe_testing/required_files/PAKDDCDF.csv"));

		viewMap = new ConcurrentHashMap<String, LinkedList>(100000, 0.75f, 100);
		// LinkedList<Document> viewList;
		LinkedList<String> viewList;
		// LinkedList<Document> listDoc;
		String msg = "";
		String key = "";
		Document msgDoc;
		String s[] = null;
		int count = 0;
		int count_T = 0;
		long start = 0L;
		brView.readLine();
		while ((msg = brView.readLine()) != null)// && count<12)
		{
			s = msg.split(",");
			if (s[13].equals("view")) {
				/*
				 * msgDoc = new Document(); msgDoc.append("server_time", s[0]);
				 * msgDoc.append("device", s[1]); msgDoc.append("session_id",
				 * s[2]); msgDoc.append("uid", s[3]); msgDoc.append("price",
				 * s[4]); msgDoc.append("category1", s[5]);
				 * msgDoc.append("category2", s[6]); msgDoc.append("category3",
				 * s[7]); msgDoc.append("category4", s[8]);
				 * msgDoc.append("brand", s[9]); msgDoc.append("item_id",
				 * s[10]); ////System.out.println(msgDoc); key =
				 * msgDoc.getString("uid");
				 */
				key = s[3];
				if (viewMap.containsKey(key)) {
					viewMap.get(key).add(msg);
				} else {
					viewList = new LinkedList<String>();
					viewList.add(msg);
					viewMap.put(key, viewList);
				}
				count++;

				if (count % 2000000 == 0) {
					start = System.currentTimeMillis();
					viewStartThreads(executors);
					// viewMap = new ConcurrentHashMap<String,LinkedList>();
					// System.out.println("Check "+count+" "+viewMap.size());
					viewMap.clear();
					// System.out.println("Time for this
					// batch="+(System.currentTimeMillis()-start));
				}

				/*
				 * for(String uid:viewMap.keySet()) { count_T++;
				 * //System.out.println("Count_T="+count_T); Thread thread = new
				 * Thread(new ViewRunnable(uid)); executors.submit(thread);
				 * thread.join(); }
				 */

				// Thread.currentThread().join();
			}
		}
		if (viewMap.size() != 0) {
			start = System.currentTimeMillis();
			viewStartThreads(executors);
			// viewMap = new ConcurrentHashMap<String,LinkedList>();
			// System.out.println("Check "+count+" "+viewMap.size());
			viewMap.clear();
			// System.out.println("Time for this
			// batch="+(System.currentTimeMillis()-start));
		}
		brView.close();
	}

	/**
	 * This method spawn thread for each user by calling ViewRunnable_Enseble method internally. 
	 * 
	 * @param executors
	 *            Pool of executors required in multi-threading.
	 * @throws  InterruptedException
	 */
	public static void viewStartThreads(ExecutorService executors) throws InterruptedException {
		int size = viewMap.keySet().size();
		int i = 0;
		/**
		 * This Field creates completion as CompletionService object used for synchronized 
		 * multi-threading.
		 */
		CompletionService<Boolean> completion = new ExecutorCompletionService(executors);
		for (String uid : viewMap.keySet()) {
			i++;
			Thread thread = new Thread(new ViewRunnable_Enseble(uid));
			Boolean result = false;
			// executors.submit(thread);
			completion.submit(thread, result);
		}
		for (i = 0; i < size; ++i) {
			completion.take(); // will block until the next sub task has
								// completed.
		}
		// System.out.println("Check2 ");
	}
	// ************************************************** API0 One Hot Vector
	// ***********************************************************************

	/**
	 * This method creates hot vector for all user by taking user's feature
	 * dictionary as input and dump hot vector in JSON file.
	 *
	 * 
	 * @throws IOException
	 */
	public static void createOneHotVector() throws IOException {
		BufferedReader brImp = new BufferedReader(
				new FileReader("/D/mukund/iPrescribe_testing/required_files/PAKDDCDF.csv"));

		BufferedReader brColsFile = new BufferedReader(
				new FileReader("/D/mukund/iPrescribe_testing/required_files/271columns.csv"));
		BufferedWriter brWriteHotVector = new BufferedWriter(
				new FileWriter("/D/mukund/iPrescribe_testing/required_files/IgniteOneHotVector.json"));
		String colName = brColsFile.readLine();
		String[] colNames = colName.split(",");
		// LinkedList<Document> orderDoc;
		String msg = "";
		String uid = "";
		Document msgDoc;
		String s[] = null;
		int count = 0;
		int count_T = 0;
		long start = 0L;
		brImp.readLine();
		Document globalDictWithoutUid;
		Document globalDict;
		while ((msg = brImp.readLine()) != null)// && count<1000000)
		{
			s = msg.split(",");
			if (s[13].equals("impression")) {
				msgDoc = new Document();
				msgDoc.append("impression_id", s[4]);
				msgDoc.append("server_time", s[0]);
				msgDoc.append("uid", s[3]);
				msgDoc.append("platform", s[21]);
				msgDoc.append("inventory_type", s[15]);
				msgDoc.append("app_code", s[14]);
				msgDoc.append("os_version", s[20]);
				msgDoc.append("model", s[18]);
				msgDoc.append("network", s[19]);
				msgDoc.append("is_click", s[16]);
				msgDoc.append("is_conversion", s[17]);
				//// System.out.println(msgDoc);
				uid = msgDoc.getString("uid");
				// System.out.print(uid);
				globalDictWithoutUid = new Document()
						.parse(PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.XGBOOSTCache.get(uid)); // Not
																								// including
																								// User_ID
																								// +
																								// uid
				globalDict = new Document().append("UserID_" + uid, globalDictWithoutUid);
				Document feature_vector = makeFeatureVector(msgDoc, globalDict);
				Document hotVector = makeHotVector(feature_vector, colNames);
				hotVector.append("uid", uid);
				brWriteHotVector.write(hotVector.toJson() + "\n");
			}
		}
		brImp.close();
		brWriteHotVector.close();
		brColsFile.close();
	}

	// ************************************************************************************************************************************
	/**
	 * This method returns the name of the feature key having maximum count.
	 * This is used when creating one hot vector.
	 *
	 * @param document
	 *            containing a user's feature dictionary for XGBOOST hot vector
	 *            creation.
	 * @return returns the feature key with maximum count value
	 */
	public static String getMaxValuedKey(Object document) {
		// TODO Auto-generated method stub

		String maxKey = null;
		if (document.getClass().toString().equals("class org.bson.Document")) {

			int maxValue = Integer.MIN_VALUE;
			Set<String> keySet = ((Document) document).keySet();
			Iterator iter = keySet.iterator();
			while (iter.hasNext()) {
				String key = (String) iter.next();
				//// System.out.println(((Document)document).get(key).getClass().toString());
				if (((Document) document).get(key).getClass().toString().equals("class java.lang.Integer")
						&& maxValue < (int) ((Document) document).get(key))
					maxKey = key;
			}

		}

		//// System.out.println("printing key in getMax: "+maxKey);
		return maxKey;
	}

	/**
	 * This method returns hot vector for a user taking user's feature
	 * dictionary as input.
	 *
	 * @param feature_vector
	 *            containing a user's feature dictionary for XGBOOST hot vector
	 *            creation.
	 * @param colNames
	 *            takes the name of the columns of which hot vector has to be
	 *            made.
	 * @return returns the hot vector for a user
	 */
	public static Document makeHotVector(Document feature_vector, String[] colNames) {
		Document hotVector = new Document();
		Set<String> keys = feature_vector.keySet();
		int count = 0;
		for (String string : colNames) {
			count++;
			//// System.out.println("string is: "+string);
			if (keys.contains(string)) {
				hotVector.append(string, (Integer) feature_vector.get(string));
			} else {
				String temp = (String) feature_vector.get(string);
				if (temp != null)
					hotVector.append(string + "_" + temp, 1);
				else
					hotVector.append(string, 0);
			}
		}
		//// System.out.println("count is: "+ count);
		return hotVector;
	}

	/**
	 * This method returns feature vector for a user taking user's feature
	 * dictionary as input. This is feature extraction.
	 * 
	 * @param GlobalDoc
	 *            containing a user's feature dictionary for XGBOOST hot vector
	 *            creation.
	 * @param imp_doc
	 *            takes the real-time messages that has come from User's portal.
	 * @return returns the feature vector for a user
	 */
	public static Document makeFeatureVector(Document imp_doc, Document GlobalDoc) {
		Document sampleDoc = new Document();
		Document UserDoc = new Document();
		Document DateUserDoc = new Document();
		Document Temp = new Document();
		// Document GlobalDoc = (Document)doc.get("Date");

		//// System.out.println((String)imp_doc.getAs("app_code"));

		sampleDoc.append("app_code", (String) imp_doc.get("app_code"));
		sampleDoc.append("network", (String) imp_doc.get("network"));
		sampleDoc.append("os_version", (String) imp_doc.get("os_version"));
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date start_date = null;
		;
		try {
			start_date = df.parse((String) imp_doc.get("server_time").toString());
			//// System.out.println(df2.format(start_date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String dateStr = df2.format(start_date).toString();
		//// System.out.println(dateStr);
		Set<String> keys = GlobalDoc.keySet();

		if (keys.contains(dateStr)) {
			UserDoc = GlobalDoc;
			DateUserDoc = (Document) GlobalDoc.get(dateStr);
			Temp = (Document) GlobalDoc.get("All");
			DateUserDoc.putAll(Temp);

			Set DocKeySet = DateUserDoc.keySet();
			Iterator DocIter = DocKeySet.iterator();

			while (DocIter.hasNext()) {
				String DocKey = (String) DocIter.next();
				if (!DateUserDoc.get(DocKey).getClass().toString().equals("class org.bson.Document")) {
					if (DocKey.contains("view"))
						sampleDoc.append(DocKey, DateUserDoc.get(DocKey));

					if (DocKey.contains("Count") && DocKey.contains("Window")) {
						if (DocKey.contains("order")) {
							for (int i = 0; i < atleastOrderThresholds.length; i++) {
								if ((int) DateUserDoc.get(DocKey) >= atleastOrderThresholds[i])
									sampleDoc.append(DocKey + "_Atleast_" + atleastOrderThresholds[i], 1);
								else
									sampleDoc.append(DocKey + "_Atleast_" + atleastOrderThresholds[i], 0);
							}
						}
					}

				} else {
					if (!highCardColumns.contains(DocKey) && !DocKey.contains("view")) {
						String[] keySplit = DocKey.split("_");
						int len = keySplit.length;
						keySplit[len - 1] = "Main_" + keySplit[len - 1];
						String mainKey = String.join("_", keySplit);
						sampleDoc.append(mainKey, getMaxValuedKey((Document) DateUserDoc.get(DocKey)));
					}
				}

			}
			//// System.out.println(sampleDoc);
			//// System.out.println("Check");
		}
		return sampleDoc;
	}

	/**
	 * This method return the All feature of feature dictionary with values initialized to initial values.
	 * @return returns the All feature of feature dictionary.
	 */
	public static Document AllDictionary() {
		String s = "{'All': {'imp_NeverClicked': 1, 'imp_NeverConverted': 1, 'imp_Count_Weekday': 0, 'imp_Count_Hour': {},"
				+ "'imp_Count_AppCode': {}, 'imp_Count_Network': {}, 'imp_Count_OsVersion': {},"
				+ "'view_Count_Weekday': 0, 'view_Count_Hour': {}, 'view_Count_Category1': {},"
				+ "'view_Count_Category2': {}, 'view_Count_Category3': {}, 'view_Count_Category4': {},"
				+ "'view_Count_Device': {}, 'order_Count_Weekday': 0, 'order_Count_Hour': {},"
				+ "'order_Count_Category1': {}, 'order_Count_Category2': {}, 'order_Count_Category3': {},"
				+ "'order_Count_Category4': {}, 'order_Count_Device': {}}}";
		Document doc = new Document().parse(s);
		return doc;
	}

	/**
	 * This method return the Window based feature of feature dictionary with values initialized to initial values.
	 * @return returns the window based feature of feature dictionary.
	 */
	public static Document windowDictionary() {
		String s = "{'imp_Count_Window3days':0, 'imp_Count_Window7days':0, 'imp_Count_Window14days':0, 'imp_Count_Window30days':0,"
				+ "'imp_Count_Window45days':0, 'imp_Count_Click_Window3days':0, 'imp_Count_Click_Window7days':0,"
				+ "'imp_Count_Click_Window14days':0, 'imp_Count_Click_Window30days':0, 'imp_Count_Click_Window45days':0,"
				+ "'imp_Count_Conversion_Window3days':0, 'imp_Count_Conversion_Window7days':0, 'imp_Count_Conversion_Window14days':0,"
				+ "'imp_Count_Conversion_Window30days':0, 'imp_Count_Conversion_Window45days':0, 'view_Count_Window3days': 0, 'view_Count_Window7days': 0, 'view_Count_Window14days': 0,"
				+ "'view_Count_Window30days': 0, 'view_Count_Window45days': 0, 'view_Count_Session_Window3days': 0,"
				+ "'view_Count_Session_Window7days': 0, 'view_Count_Session_Window14days': 0, 'view_Count_Session_Window30days': 0,"
				+ "'view_Count_Session_Window45days': 0, 'order_Count_Window3days': 0, 'order_Count_Window7days': 0, 'order_Count_Window14days': 0,"
				+ "'order_Count_Window30days': 0, 'order_Count_Window45days': 0, 'order_Count_Session_Window3days': 0,"
				+ "'order_Count_Session_Window7days': 0, 'order_Count_Session_Window14days': 0, 'order_Count_Session_Window30days': 0,"
				+ "'order_Count_Session_Window45days': 0," + "}";

		Document doc = new Document().parse(s);
		return doc;
	}

}
