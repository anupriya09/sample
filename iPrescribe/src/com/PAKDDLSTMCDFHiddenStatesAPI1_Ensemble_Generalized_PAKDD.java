package com;


import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamVisitor;
import org.apache.ignite.stream.kafka.KafkaStreamer;
import org.bson.Document;
import org.jblas.FloatMatrix;
import org.jblas.MatrixFunctions;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table.Cell;
//import com.google.gson.Gson;

import kafka.consumer.ConsumerConfig;
import kafka.message.MessageAndMetadata;

import static com.XGBLSTMLoaderMain.dict;
import static com.XGBLSTMLoaderMain.featfunc;
import static com.XGBLSTMLoaderMain.metafile2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD {
	
	public static final String	CONSUMER_GROUP_ID_PROPERTY		= "group.id";
	public static final String	ZOOKEEPER_CONNECT_PROPERTY		= "zookeeper.connect"	;
	public static final String	BOOTSTRAP_SERVERS_PROPERTY		= "bootstrap.servers"	;
	public static final String	AUTO_OFFSET_RESET_PROPERTY		= "auto.offset.reset";
	
	public static final String	CONSUMER_GROUP_ID_VALUE_PRED	= "IgniteGroup_1";
	public static final String	CONSUMER_GROUP_ID_VALUE_IMP		= "ImpTableGrp_1";
	//public static final String	ZOOKEEPER_CONNECT_VALUE			= "192.168.140.46:2181"					;
	//public static final String	BOOTSTRAP_SERVERS_VALUE			= "192.168.140.46:9092"					;
	public static final String	ZOOKEEPER_CONNECT_VALUE			= "127.0.0.1:2181"					;
	public static final String	BOOTSTRAP_SERVERS_VALUE			= "127.0.0.1:9092"					;
	public static final String	AUTO_OFFSET_RESET_VALUE			= "smallest";
	public static final String	TOPIC_NAME_PRED			= "PAKDD_27_low_rate";
	static IgniteDataStreamer<String, String> 	tradeStreamer;
	public static final String	TOPIC_NAME_IMP			= "Imp_Table";
	static IgniteDataStreamer<String, String> 	ImpStreamer;
	
	//final static String filePath = "/D/mukund/iPrescribe_testing/required_files/ANVETION/";
	//final static String outFilePath = "/home/sharod/";
	final static String filePath = "/root/";
	final static String outFilePath = "/root/";
	//final static String haProxy = "http://192.168.140.52:8080";
	//final static String haProxyHost= "192.168.140.52";
	//final static int haProxyPort = 8080;
	final static String haProxy = "http://127.0.0.1:8881";
	final static String haProxyHost= "127.0.0.1";
	final static int haProxyPort = 8881;
	
	static PreProcessCSV metafile1 = new PreProcessCSV(filePath+"PAKDDmetadataChanged.csv");
	static PreProcessMetafile2v2 metafile2= new PreProcessMetafile2v2(filePath+"PAKDDfeatMetafileCopy.csv");
	//static PreProcessCSV metafile1 = new PreProcessCSV("/home/sanket/workspace_python/iPrescribeGeneralization/instacartMetadataChanged.csv");
	//static PreProcessMetafile2v2 metafile2= new PreProcessMetafile2v2("/home/sanket/workspace_python/iPrescribeGeneralization/instacartFeatMetafile - Copy.csv");
	
	static FeatureFuncV2 featfunc = new FeatureFuncV2();
	static FileWriter trainingfile ;
    static FileReader trainingSetColNames;
    static List<Integer> windowsConstant = Arrays.asList(3,7,14,30,60,90,120,150,180,210,240,270,300,330,360);
    static List<Integer> windows = new ArrayList<>();
    static int days;
    static DictionaryFunc dict = new DictionaryFunc();
    static  DateTimeFormatter dfInCDF = DateTimeFormat.forPattern(metafile1.getCellValueForParticularRow("server_time", "Information"));
    static  DateTimeFormatter dft = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static int[] atleastThresolds = {2, 5, 10, 16, 35};
    public static CacheConfiguration<String, String> LSTMCfg;
    public static IgniteDataStreamer<String, String> LSTMStreamer;
	//public static PrintWriter ignite_time;
	
	public static void main(String[] args) throws Exception {
		long start,start1,end;
		trainingSetColNames = new FileReader(filePath+"TrainingSetColNamesPakdd_Sanket.txt");
	    //trainingSetColNames = new FileReader("/home/sanket/workspace_python/iPrescribeGeneralization/TrainingSetColNamesInsta.txt");
		
		days = Integer.parseInt(metafile1.getCellValueForParticularRow("windows", "Information"));
	    //ignite_time = new PrintWriter(new FileOutputStream("/D/mukund/iPrescribe_testing/required_files/ANVETION/ignite_time.txt",true));
	    
	    for (int addWindow : windowsConstant)
	        if (addWindow <= days)
	            windows.add(addWindow);
	    //System.out.println(windows);
	  		
		BufferedReader br = new BufferedReader(trainingSetColNames);
        String line;
        ArrayList<String> colNames = new ArrayList<>();
        while((line = br.readLine()) != null)
            colNames.add(line);
        br.close();
        
        /////////////////////////////////////lstm//////////////////////////////
        BufferedReader configread;
		configread = new BufferedReader(
				new FileReader(filePath+"PAKDDConfigNew2.csv"));
		Map<String, ArrayList<String>> headFuncDict;
		Map<String, String> typedict;
		IgniteCache<String, String> LSTMCache;
		IgniteCache<String, String> XGBOOSTCache;
		//IgniteCache<String, String> buyingProbCache;
		ArrayList<String> funcType;
		ArrayList<String> catHeaderList = new ArrayList<String>();
		Set<String> users = new HashSet<String>();
		ArrayList<String> ytrain = new ArrayList<String>();
		funcType = new ArrayList<String>();
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
		funcType.add("TypeO");
		String columnLevels[] = { "uid" };

		Object[] confdicts = configDicts(configread, funcType);
		headFuncDict = (Map<String, ArrayList<String>>) confdicts[0];
		typedict = (Map<String, String>) confdicts[1];
		BufferedReader fr = new BufferedReader(new FileReader(filePath+"weightsasdict.txt"));
		String sfr=fr.readLine();
		Document weights=new Document().parse(sfr);
		///////////////////////////////////////////////////////////////////////////
		
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
		 
		TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

 	    ipFinder.setAddresses(
				Arrays.asList("127.0.0.1:47500..47516"));//,"192.168.140.48:47500..47516"));//,"192.168.140.49:47500..47516")); 
				//Arrays.asList("192.168.140.44:47500..47509","192.168.140.48:47500..47509"));
		spi.setIpFinder(ipFinder);
		
		final IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
		igniteConfiguration.setClientMode(true);
		igniteConfiguration.setPeerClassLoadingEnabled(true);
		igniteConfiguration.setSystemThreadPoolSize(60000);
		igniteConfiguration.setDiscoverySpi(spi);
		
		
		Ignite ignite = Ignition.start(igniteConfiguration);
		System.out.println("This is Client......");
		CacheConfiguration<String, String> TestDataCfg = new CacheConfiguration<>("XGBCache");
		TestDataCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		 IgniteCache<String,String> GenCache = ignite.getOrCreateCache(TestDataCfg);
		 tradeStreamer= ignite.dataStreamer(GenCache.getName());
		 //tradeStreamer.autoFlushFrequency(1000);
		 tradeStreamer.perNodeBufferSize(1);
		 tradeStreamer.allowOverwrite(true);
		 tradeStreamer.perNodeParallelOperations(3);
		 
		LSTMCfg = new CacheConfiguration<>("LSTM");
		LSTMCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
		LSTMCache = ignite.getOrCreateCache(LSTMCfg);

		LSTMStreamer = ignite.dataStreamer(LSTMCache.getName());
		//LSTMStreamer.autoFlushFrequency(1);
		LSTMStreamer.perNodeBufferSize(1);
		LSTMStreamer.allowOverwrite(true);
		LSTMStreamer.perNodeParallelOperations(3);
		 
		 
		 tradeStreamer.receiver(StreamVisitor.from((cache, e) -> {
			    long start12=System.currentTimeMillis();
			    PrintWriter ignite_time=null;
			    PrintWriter dashboard=null;
			    String recordTemp = e.getValue();
				//System.out.println("here1");
				Document record = Document.parse(recordTemp);
				String seq_num = record.getString("seq_num");
				record.remove("seq_num");
				try {
					ignite_time = new PrintWriter(new FileOutputStream(outFilePath+"ignite_time.txt",true));
					if(Integer.parseInt(seq_num)<45)
						dashboard = new PrintWriter(new FileOutputStream(outFilePath+"dashboard.txt",true));
				} catch (FileNotFoundException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
				
				String key1 = record.getString("uid");
				if(((String)record.get("transaction_type")).equals("impression") || Integer.parseInt(seq_num)>44)
				{
					
					if(Integer.parseInt(seq_num)<45)
					{
						System.out.println("Generating Offer........");
						dashboard.write("User:"+key1+" Searching for Offer........\n");
					}
        		
        		//start = System.currentTimeMillis();
        		//System.out.println(cache.get(key1).getClass());
        		long start13=System.currentTimeMillis();
        		Document globalDict_new = Document.parse(cache.get(key1));
        		//System.out.println(cache.Ent
        		List<String> initialList = Arrays.asList( metafile1.getCellValueForParticularRow("eval_set", "Initials").split(","));
        		LocalDate DateValue = dfInCDF.parseLocalDate(record.getString("server_time"));
        		
        		
        		//System.out.println("Time fetching Dictionary=,"+(start13-start12));
        		ArrayList<String> featVec = makeFeatureVector(globalDict_new, record, initialList, DateValue);
        		long start14=System.currentTimeMillis();
        		//System.out.println("Time Feature Vector=,"+(start14-start13));
        		//System.out.println("FeatVec="+featVec.size());
        		ArrayList<String> HotVectorList = makeHotVector(featVec, colNames);
        		long start15=System.currentTimeMillis();
        		//System.out.println("Time Hot Vector=,"+(start15-start14));
        		
        		HttpPost httpPost = new HttpPost(haProxy);
        		
   //*********************************************** HAS RECONSTRUCTED ************************************************************//
        		Document hotvectorlist = new Document("uid", key1);
				hotvectorlist.append("List", HotVectorList);
				hotvectorlist.append("seq_num", seq_num);
        		
//*********************************************** LSTM ************************************************************//	
				
				
				Document d=new Document();
				try {
					//long startlstm = System.currentTimeMillis();
					d=createCdfDictUltimate(record,headFuncDict,typedict,catHeaderList,LSTMCache,weights);
					//System.out.println("Full LSTM Time in ms="+(System.currentTimeMillis()-startlstm));
					ignite_time.println("Seq_num=,"+seq_num+",Time in ignite=,"+(System.currentTimeMillis()-start13));
	        		//ignite_time.close();
					hotvectorlist.append("PAKDD_LSTM", d);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
//************************************************************************************************************************************				
				
				try {
					httpPost.setEntity(new StringEntity(hotvectorlist.toJson()));
				} catch (UnsupportedEncodingException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				try {
					//CloseableHttpResponse closeableResponse = Singleton.getInstance().httpClient.execute(httpPost);
					Singleton.getInstance().httpClient.execute(httpPost);
					//System.out.println("Ignite Time in ms=,"+ (System.currentTimeMillis()-start15)+",# of Products/User=,"+HotVectorList.size());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        		//ignite_time.println("Seq_num=,"+seq_num+",Time in ignite=,"+(System.currentTimeMillis()-start15));
        		//ignite_time.close();
				}
				
				else if(((String)record.get("transaction_type")).equals("view"))
				{
						System.out.println("User:"+key1+" has viewed the product");
						dashboard.write("User:"+key1+" has viewed the product --> "+record.getString("item_id")+"\n");
				}
				else
				{
						System.out.println("User:"+key1+" has ordered the product");
						dashboard.write("User:"+key1+" has ordered the product --> "+record.getString("item_id")+"\n");
				}
				//ignite_time.flush();
				if(Integer.parseInt(seq_num)<45)
					dashboard.close();
				ignite_time.close();
        	}));
		 
		 Properties kafkaProperties  = new Properties();
		kafkaProperties.put(PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.CONSUMER_GROUP_ID_PROPERTY, 
				PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.CONSUMER_GROUP_ID_VALUE_PRED);

		kafkaProperties.put(PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.ZOOKEEPER_CONNECT_PROPERTY, 
				PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.ZOOKEEPER_CONNECT_VALUE);

		kafkaProperties.put(PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.BOOTSTRAP_SERVERS_PROPERTY, 
				PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.BOOTSTRAP_SERVERS_VALUE);

		kafkaProperties.put(PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.AUTO_OFFSET_RESET_PROPERTY, 
				PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.AUTO_OFFSET_RESET_VALUE);	
		
		KafkaStreamer<String, String> kafkaStreamer = new KafkaStreamer<>();
		kafkaStreamer.setIgnite(ignite)	;
		kafkaStreamer.setStreamer(tradeStreamer);
		
		// set the topic
		kafkaStreamer.setTopic(PAKDDLSTMCDFHiddenStatesAPI1_Ensemble_Generalized_PAKDD.TOPIC_NAME_PRED);

		// set the number of threads to process Kafka streams
		kafkaStreamer.setThreads(1);
		ConsumerConfig kafkaConsumerConfig = new ConsumerConfig(kafkaProperties);

		kafkaStreamer.setConsumerConfig(kafkaConsumerConfig);		 
		kafkaStreamer.setMultipleTupleExtractor(
                new StreamMultipleTupleExtractor<MessageAndMetadata<byte[], byte[]>, String, String>() {
                @Override 
                public Map<String, String> extract(MessageAndMetadata<byte[], byte[]> msg) {
                	String 	 data 	= new String(msg.message())	;
                	String seq_num = new String(msg.key());
                	System.out.println("seq_num="+seq_num);
                	Document imp_doc = new Document().parse(data);
                	imp_doc.append("seq_num", seq_num);
                	Map<String, String> 	entries = new HashMap<>();	
                	entries.put((String)imp_doc.get("uid"), imp_doc.toJson());
                	//entries.put(seq_num, imp_doc.toJson());
			        return entries	;																	
                }
            });	
		
		kafkaStreamer.start();	
	}
	
	
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
		if (columnValue.length() == 0 || columnValue.equals("NA")) {
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
	
	public static float[][] tofloatmatrix(ArrayList<ArrayList<Double>> lst) {
		float [][] floatValues = new float[lst.size()][lst.get(0).size()];
		
		//System.out.println(type(lst.get(0)));
		for (int i = 0; i < lst.size(); i++) {
			for (int j = 0; j < lst.get(0).size(); j++) {
				floatValues[i][j] = lst.get(i).get(j).floatValue();
			}
		}
		return floatValues;
	}
	
	public static float[] tofloatarray(ArrayList<Double> lst) {
		float [] floatValues = new float[lst.size()];
		
		//System.out.println(type(lst.get(0)));
		for (int i = 0; i < lst.size(); i++) {
			
				floatValues[i] = lst.get(i).floatValue();
			
		}
		return floatValues;
	}
	
	public static float[][] tofloatarrayfs(ArrayList<ArrayList<Double>> lst) {
		float [][] doubleValues = new float[1][lst.get(0).size()];

		for (int i = 0; i < lst.get(0).size(); i++) {
		    doubleValues[0][i] = lst.get(0).get(i).floatValue();
		}
		return doubleValues;
	}
	
	
        		
    public static FloatMatrix hard_sigmoid(FloatMatrix x) {
		
		x=x.mmul((float) 0.2);
		x=x.add((float) 0.5);
		FloatMatrix zeros=FloatMatrix.zeros(x.length);
		FloatMatrix ones=FloatMatrix.ones(x.length);
		FloatMatrix xmin=x.min(ones);//Transforms.min(ones, x);
		x=xmin.max(zeros);//Transforms.max(zeros, xmin);
		return x;
		
	}
    
    public static double[] softmax(FloatMatrix x) {
//    	System.out.println("insoftmax");
//    	System.out.println(x.columnMaxs());
//    	System.out.println(x.rowMaxs());
//    	System.out.println(x);
//    	System.out.println(x.subColumnVector(x.rowMaxs()));
    	//FloatMatrix e=MatrixFunctions.exp(x.subColumnVector(x.rowMaxs()));
    	
    	//System.out.println(e);
    	FloatMatrix ee=x.subColumnVector(x.rowMaxs());
    	double[] dd=new double[ee.columns];
    	double[] e=new double[ee.columns];
    	double rowsum=0.0;
    	for(int i=0;i<ee.columns;i++) {
    		dd[i]=ee.get(i);
    		e[i]=Math.exp(dd[i]);
    		rowsum+=e[i];
    	}
    	double[] output=new double[ee.columns];
    	for(int i=0;i<ee.columns;i++) {
    		output[i]=e[i]/rowsum;
    	}
//    	for()
//    	double d=ee.get(0);
//    	System.out.println("exp");
//    	System.out.println(Math.exp(d));
//    	FloatMatrix s=e.rowSums();
//    	System.out.println(s);
//    	return e.divRowVector(s);
    	return output;
    }
    
    public static double[] mypredictjavajblas(Document weights,ArrayList<ArrayList<String>> input, ArrayList<ArrayList<Double>> h, ArrayList<ArrayList<Double>> c, int units) throws FileNotFoundException {
		//System.out.println("inside mypredict");
    	ArrayList<ArrayList<String>> actualval=input;
    	//System.out.println("a");
		//float[] actualvalfloat=tofloatarray(actualval);
		//System.out.println(actualval.size());
		float[][] floatinput=new float[1][actualval.size()];
		//System.out.println("b");
		//System.out.println(actualval);
		//System.out.println(input);
		for(int a=0;a<actualval.size();a++) {
			//System.out.println(a);
			//System.out.println(actualval.get(a));
			//System.out.println(actualval.get(a).get(0));
			floatinput[0][a]=Float.parseFloat(actualval.get(a).get(0));
		}
		//System.out.println("c");
		//System.out.println("checking jblas");
		FloatMatrix inputjblas=new FloatMatrix(floatinput);
		//System.out.println("jblas ok");
		ArrayList<ArrayList<Double>> lstm_1kernel0=(ArrayList<ArrayList<Double>>) weights.get("lstm_1/kernel:0");
		FloatMatrix lstm_1kernel0jblas=new FloatMatrix(tofloatmatrix(lstm_1kernel0));
		//System.out.println("weights and tfm ok");
		//inputjblas=inputjblas.transpose();
//		System.out.println(inputjblas.columns);
//		System.out.println(inputjblas.rows);
//		System.out.println(lstm_1kernel0nd4j.columns);
//		System.out.println(lstm_1kernel0nd4j.rows);
		FloatMatrix lstm_input=inputjblas.mmul(lstm_1kernel0jblas);
//		System.out.println("lstminput");
//		System.out.println(lstm_input);
		
		ArrayList<ArrayList<Double>> lstm_1recurrent_kernel0=(ArrayList<ArrayList<Double>>) weights.get("lstm_1/recurrent_kernel:0");
		FloatMatrix lstm_1recurrent_kernel0jblas=new FloatMatrix(tofloatmatrix(lstm_1recurrent_kernel0));
//		PrintWriter fout=new PrintWriter(new File("outputter.txt"));
//		fout.println("act h");
//		fout.println(h);
		FloatMatrix hjblas=new FloatMatrix(tofloatarrayfs(h));
		FloatMatrix cjblas=new FloatMatrix(tofloatarrayfs(c));
//		fout.println("h");
//		fout.println(hnd4j);
		FloatMatrix hidden_input=hjblas.mmul(lstm_1recurrent_kernel0jblas);
		//System.out.println("hidden input done");
		
		
//		fout.println("hiddeninput");
//		fout.println(hidden_input);
		
		ArrayList<Double> lstm_1bias0=(ArrayList<Double>) weights.get("lstm_1/bias:0");
		FloatMatrix lstm_1bias0jblas=new FloatMatrix(tofloatarray(lstm_1bias0));
//		System.out.println(hidden_input.columns);
//		System.out.println(hidden_input.rows);
		//FloatMatrix zz=lstm_input.add(hidden_input);
		
		FloatMatrix zz = hidden_input.addRowVector(lstm_input);
		FloatMatrix z = zz.addRowVector(lstm_1bias0jblas);
		//System.out.println("finding z");
//		fout.println("z");
//		fout.println(z);
//		fout.close();
//		System.out.println("z");
//		System.out.println(z.columns);
//		System.out.println(z.rows);
		FloatMatrix z0 = z.getColumnRange(0,0,units);//get(NDArrayIndex.all(), NDArrayIndex.interval(0,units));
		FloatMatrix z1 = z.getColumnRange(0,units,2*units);//get(NDArrayIndex.all(), NDArrayIndex.interval(units,2 * units));
		FloatMatrix z2 = z.getColumnRange(0,2*units,3*units);//get(NDArrayIndex.all(), NDArrayIndex.interval(2 * units,3 * units));
		FloatMatrix z3 = z.getColumnRange(0,3*units,z.columns);//get(NDArrayIndex.all(), NDArrayIndex.interval(3 * units,4 * units));
		
		FloatMatrix i = hard_sigmoid(z0);
		FloatMatrix f = hard_sigmoid(z1);
		cjblas=f.mul(cjblas).add(i.mul(MatrixFunctions.tanh(z2)));

		FloatMatrix o = hard_sigmoid(z3);
		hjblas = o.mul(MatrixFunctions.tanh(cjblas));
		//System.out.println("gates done");
		
		ArrayList<ArrayList<Double>> dense_1kernel0=(ArrayList<ArrayList<Double>>) weights.get("dense_1/kernel:0");
		FloatMatrix dense_1kernel0jblas=new FloatMatrix(tofloatmatrix(dense_1kernel0));
		
		ArrayList<Double> dense_1bias0=(ArrayList<Double>) weights.get("dense_1/bias:0");
		FloatMatrix dense_1bias0jblas=new FloatMatrix(tofloatarray(dense_1bias0));
		
		FloatMatrix dense=hjblas.mmul(dense_1kernel0jblas);
		dense=dense.addRowVector(dense_1bias0jblas);
 		//System.out.println("dense done");
//		System.out.println(dense);
		double[] output=softmax(dense);
		//System.out.println("softmax done");
//		System.out.println(output[0]);
//		System.out.println(output[1]);
//		
		return output;
	}

    
	
	public static Document createCdfDictUltimate(Document cdf,Map<String,ArrayList<String>> headFuncDict, Map<String,String> typedict, ArrayList<String> catHeaderList, IgniteCache<String,String> TestCache, Document weights)throws IOException {
		Document lstmData=new Document();
		//cdfMap = new ConcurrentHashMap<String,LinkedList>(100000,0.75f,100);
		//LinkedList<Document> viewList;
		LinkedList<String> cdfList;
		//LinkedList<Document> listDoc;createCdfDict
		Document d=new Document();
		String msg="";
		String key="";
		String s[]=null;
		int count=0;
		String columnLevels[]= {"uid"};
		//System.out.println("entered 1");
		//long startdict=System.currentTimeMillis();
		
		
		
		if(columnLevels.length==2) {
			
		}else {
			//System.out.println("entered 2");
			String cdfColLev1 = "";
			catHeaderList=new ArrayList<String>();
			//System.out.println("headfuncdict new");
			//System.out.println(headFuncDict.size());
			for (Map.Entry<String,ArrayList<String>> entry : headFuncDict.entrySet()) {
				
				ArrayList<String> functionList=entry.getValue();
				for(String func:functionList) {
					if(func.equals("TypeI")){
						catHeaderList.add(entry.getKey());
					}
			    //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
				}
			}
		
		
		
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			int eventNumber=0;
			String currentLevel0=cdf.get("uid").toString();

			for (Map.Entry<String,ArrayList<String>> entry : headFuncDict.entrySet()) {
				//System.out.println("entered2");
				ArrayList<String> functionList=entry.getValue();
				for(String func:functionList) {
					//System.out.println("entered3");
					String fillerValue="";
					if(func.equals("TypeN")){
						lstmData=directfromDF(lstmData, currentLevel0, cdfColLev1, cdf.get(entry.getKey()).toString(), entry.getKey(), eventNumber, fillerValue,catHeaderList,true,typedict);
					}
					if(func.equals("TypeO")){
						//System.out.println()
						lstmData=transactionType(lstmData, currentLevel0, cdfColLev1,cdf.get(entry.getKey()).toString(), entry.getKey(), eventNumber);
					}
			    //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
				}
			}//System.out.println(lstmData);  
			//long enddict = System.currentTimeMillis();
			//System.out.println(enddict-startdict);
			//long ulisttimes=System.currentTimeMillis();
			ArrayList<String> feature_names=new ArrayList<>();
			feature_names.add("category2_direct");
			feature_names.add("quantity_direct");
			feature_names.add("price_direct");
			feature_names.add("category1_direct");
			feature_names.add("os_version_direct");
			feature_names.add("device_direct");
			feature_names.add("network_direct");
			//ArrayList<ArrayList<ArrayList<String>>> sequencelist= new ArrayList<ArrayList<ArrayList<String>>>();
			Document features=(Document)lstmData.get(cdf.get("uid").toString());
			//System.out.println("StartTime of matrix creation= "+System.currentTimeMillis()+" for user "+cdf.get("uid").toString());
			//long starttimemat=System.currentTimeMillis();
			ArrayList<ArrayList<String>> userlist= new ArrayList<ArrayList<String>>();
			for(String feature:feature_names) {
				
				ArrayList<String> data=(ArrayList<String>)features.get(feature);
				
				userlist.add(data);
				
			}
			//long ulisttimee=System.currentTimeMillis();
			//System.out.println(ulisttimee-ulisttimes);
			//long cacheiers=System.currentTimeMillis();
			String thehs=TestCache.get(cdf.get("uid").toString());
			//System.out.println("entered 3");
			Document hs=new Document();
			if(thehs!=null){
				hs=new Document().parse(thehs);
			}else{
				ArrayList<Double> listh = new ArrayList<Double>(Collections.nCopies(150, 1.0));
				ArrayList<ArrayList<Double>> listhf=new ArrayList<ArrayList<Double>>();
				listhf.add(listh);
				ArrayList<Double> listc = new ArrayList<Double>(Collections.nCopies(150, 1.0));
				ArrayList<ArrayList<Double>> listcf=new ArrayList<ArrayList<Double>>();
				listcf.add(listc);
				hs.append("h", listhf);
				hs.append("c", listcf);
			}
			//System.out.println("printing h and c");
			//System.out.println((ArrayList<ArrayList<Double>>)hs.get("h"));
			//System.out.println((ArrayList<ArrayList<Double>>)hs.get("c"));
			//System.out.println("entered 4");
			//long cacheiere=System.currentTimeMillis();
			//System.out.println(cacheiere-cacheiers);
			//long weighers=System.currentTimeMillis();
			
			//long weighere=System.currentTimeMillis();
			//System.out.println(weighere-weighers);
			//FileWriter fw=new FileWriter("/home/sharod/api21Ltiming.txt",true);
			//long mypredictStartTime = System.currentTimeMillis();
			double[] output=mypredictjavajblas(weights,userlist,(ArrayList<ArrayList<Double>>)hs.get("h"),(ArrayList<ArrayList<Double>>)hs.get("c"),150);
			//System.out.println("entered 5");
			//long mypredictEndTime = System.currentTimeMillis();
			//System.out.println(mypredictEndTime-mypredictStartTime);
			//fw.write((mypredictEndTime-mypredictStartTime)+"\n");
			//fw.close();
			d=new Document();
			//d.append("user", cdf.get("uid").toString());
			//d.append("xtest",userlist);
			//d.append("hiddenstates", hs);
			d.append("lstmoutput0", output[0]);
			d.append("lstmoutput1", output[1]);
			//System.out.println(userlist);
			
			//System.out.println("EndTime of tornado= "+System.currentTimeMillis()+" for user "+uid);
			//long endtimetornado=System.currentTimeMillis();
			//System.out.println("Latency of tornado= "+Long.toString(endtimetornado-starttimetornado)+" for user "+uid);
			//System.out.println("entered 6");
		}
		return d;
		
		
	}
	
	public static Document createCdfDict(Document cdf, ExecutorService executors,
			Map<String, ArrayList<String>> headFuncDict, Map<String, String> typedict, ArrayList<String> catHeaderList,
			IgniteCache<String, String> TestCache) throws IOException {
		Document lstmData = new Document();
		// cdfMap = new ConcurrentHashMap<String,LinkedList>(100000,0.75f,100);
		// LinkedList<Document> viewList;
		LinkedList<String> cdfList;
		// LinkedList<Document> listDoc;createCdfDict
		Document d = new Document();
		String msg = "";
		String key = "";
		String s[] = null;
		int count = 0;
		String columnLevels[] = { "uid" };

		if (columnLevels.length == 2) {

		} else {
			String cdfColLev1 = "";
			catHeaderList = new ArrayList<String>();
			//System.out.println("headfuncdict new");
			//System.out.println(headFuncDict.size());
			for (Map.Entry<String, ArrayList<String>> entry : headFuncDict.entrySet()) {

				ArrayList<String> functionList = entry.getValue();
				for (String func : functionList) {
					if (func.equals("TypeI")) {
						catHeaderList.add(entry.getKey());
					}
					// System.out.println("Key = " + entry.getKey() + ", Value =
					// " + entry.getValue());
				}
			}

			//////////////////////////////////////////////////////////////////////////////////////////////////////////////

			int eventNumber = 0;
			String currentLevel0 = cdf.get("uid").toString();

			for (Map.Entry<String, ArrayList<String>> entry : headFuncDict.entrySet()) {
				//System.out.println("entered2");
				ArrayList<String> functionList = entry.getValue();
				for (String func : functionList) {
					//System.out.println("entered3");
					String fillerValue = "";
					if (func.equals("TypeN")) {
						lstmData = directfromDF(lstmData, currentLevel0, cdfColLev1, cdf.get(entry.getKey()).toString(),
								entry.getKey(), eventNumber, fillerValue, catHeaderList, true, typedict);
					}
					if (func.equals("TypeO")) {
						// System.out.println()
						lstmData = transactionType(lstmData, currentLevel0, cdfColLev1,
								cdf.get(entry.getKey()).toString(), entry.getKey(), eventNumber);
					}
					// System.out.println("Key = " + entry.getKey() + ", Value =
					// " + entry.getValue());
				}
			}
			//System.out.println(lstmData);

			ArrayList<String> feature_names = new ArrayList<>();
			feature_names.add("category2_direct");
			feature_names.add("quantity_direct");
			feature_names.add("price_direct");
			feature_names.add("category1_direct");
			feature_names.add("os_version_direct");
			feature_names.add("device_direct");
			feature_names.add("network_direct");
			// ArrayList<ArrayList<ArrayList<String>>> sequencelist= new
			// ArrayList<ArrayList<ArrayList<String>>>();
			Document features = (Document) lstmData.get(cdf.get("uid").toString());
			//System.out.println("StartTime of matrix creation= " + System.currentTimeMillis() + " for user "
					//+ cdf.get("uid").toString());
			long starttimemat = System.currentTimeMillis();
			ArrayList<ArrayList<String>> userlist = new ArrayList<ArrayList<String>>();
			for (String feature : feature_names) {

				ArrayList<String> data = (ArrayList<String>) features.get(feature);

				userlist.add(data);

			}
			Document hs = new Document().parse(TestCache.get(cdf.get("uid").toString()));

			d = new Document();
			d.append("user", cdf.get("uid").toString());
			d.append("xtest", userlist);
			d.append("hiddenstates", hs);
			//System.out.println(userlist);

			// System.out.println("EndTime of tornado=
			// "+System.currentTimeMillis()+" for user "+uid);
			// long endtimetornado=System.currentTimeMillis();
			// System.out.println("Latency of tornado=
			// "+Long.toString(endtimetornado-starttimetornado)+" for user
			// "+uid);

		}
		return d;

	}

	public static Document createCdfDict2(Document cdf, ExecutorService executors,
			Map<String, ArrayList<String>> headFuncDict, Map<String, String> typedict, ArrayList<String> catHeaderList,
			IgniteCache<String, String> TestCache) throws IOException {

		Document lstmData = new Document();
		// cdfMap = new ConcurrentHashMap<String,LinkedList>(100000,0.75f,100);
		// LinkedList<Document> viewList;
		LinkedList<String> cdfList;
		// LinkedList<Document> listDoc;createCdfDict
		Document d = new Document();
		String msg = "";
		String key = "";
		String s[] = null;
		int count = 0;
		String columnLevels[] = { "uid" };

		if (columnLevels.length == 2) {

		} else {
			String cdfColLev1 = "";
			catHeaderList = new ArrayList<String>();
			//System.out.println("headfuncdict new");
			//System.out.println(headFuncDict.size());
			for (Map.Entry<String, ArrayList<String>> entry : headFuncDict.entrySet()) {

				ArrayList<String> functionList = entry.getValue();
				for (String func : functionList) {
					if (func.equals("TypeI")) {
						catHeaderList.add(entry.getKey());
					}
					// System.out.println("Key = " + entry.getKey() + ", Value =
					// " + entry.getValue());
				}
			}

			//////////////////////////////////////////////////////////////////////////////////////////////////////////////

			int eventNumber = 0;
			String currentLevel0 = cdf.get("uid").toString();

			for (Map.Entry<String, ArrayList<String>> entry : headFuncDict.entrySet()) {
				//System.out.println("entered2");
				ArrayList<String> functionList = entry.getValue();
				for (String func : functionList) {
					//System.out.println("entered3");
					String fillerValue = "";
					if (func.equals("TypeN")) {
						lstmData = directfromDF(lstmData, currentLevel0, cdfColLev1, cdf.get(entry.getKey()).toString(),
								entry.getKey(), eventNumber, fillerValue, catHeaderList, true, typedict);
					}
					if (func.equals("TypeO")) {
						// System.out.println()
						lstmData = transactionType(lstmData, currentLevel0, cdfColLev1,
								cdf.get(entry.getKey()).toString(), entry.getKey(), eventNumber);
					}
					// System.out.println("Key = " + entry.getKey() + ", Value =
					// " + entry.getValue());
				}
			}
			//System.out.println(lstmData);

			ArrayList<String> feature_names = new ArrayList<>();
			feature_names.add("category2_direct");
			feature_names.add("quantity_direct");
			feature_names.add("price_direct");
			feature_names.add("category1_direct");
			feature_names.add("os_version_direct");
			feature_names.add("device_direct");
			feature_names.add("network_direct");
			// ArrayList<ArrayList<ArrayList<String>>> sequencelist= new
			// ArrayList<ArrayList<ArrayList<String>>>();
			Document features = (Document) lstmData.get(cdf.get("uid").toString());
			//System.out.println("StartTime of matrix creation= " + System.currentTimeMillis() + " for user "
					//+ cdf.get("uid").toString());
			long starttimemat = System.currentTimeMillis();
			ArrayList<ArrayList<String>> userlist = new ArrayList<ArrayList<String>>();
			for (String feature : feature_names) {

				ArrayList<String> data = (ArrayList<String>) features.get(feature);

				userlist.add(data);

			}
			Document hs = new Document().parse(TestCache.get(cdf.get("uid").toString()));

			d = new Document();
			d.append("user", cdf.get("uid").toString());
			d.append("xtest", userlist);
			d.append("hiddenstates", hs);
			//System.out.println(userlist);

			// System.out.println("EndTime of tornado=
			// "+System.currentTimeMillis()+" for user "+uid);
			// long endtimetornado=System.currentTimeMillis();
			// System.out.println("Latency of tornado=
			// "+Long.toString(endtimetornado-starttimetornado)+" for user
			// "+uid);

		}
		return d;

	}

	public static Object[] configDicts(BufferedReader config, ArrayList<String> availLSTMFunctionTypes)
			throws IOException {
		Map<String, ArrayList<String>> functionDict = new HashMap<String, ArrayList<String>>();
		Map<String, String> typeDict = new HashMap<String, String>();
		String row;

		while ((row = config.readLine()) != null) {
			//System.out.println("row");
			//System.out.print(row);
			String[] elements = row.split(",", 6);
			//System.out.println("\nelements");
			//System.out.println(Arrays.asList(elements));
			String functions = elements[3];
			//System.out.println("functions:" + functions);
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
		//System.out.println("functionDict");
		//System.out.println(functionDict);
		return new Object[] { functionDict, typeDict };
	}
	
static	class Singleton {

	    private static Singleton httpClientPool;

	    static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();{
		// Increase max total connection to 200
		cm.setMaxTotal(2000);
		// Increase default max connection per route to 20
		cm.setDefaultMaxPerRoute(2000);
		
		}
		// Increase max connections for localhost:80 to 50
		private HttpHost localhost = new HttpHost(haProxyHost, haProxyPort);{
		cm.setMaxPerRoute(new HttpRoute(localhost), 2000);}

		private static CloseableHttpClient httpClient = HttpClients.custom()
		        .setConnectionManager(cm)
		        .build();
	    
	    private Singleton() {
	    	
	    }

	    public static Singleton getInstance() {
	        if (httpClientPool == null) {
	        	httpClientPool = new Singleton();
	        }

	        return httpClientPool;
	    }
	}


static ArrayList<String> makeHotVector(ArrayList<String> featVecList, ArrayList<String> colNames){
    ArrayList<String> HotVectorList = new ArrayList<>();
    //System.out.println("colnames: "+colNames);
    for(String vector : featVecList){
        //System.out.println("for 1 key2---------------------------------------");
        //System.out.println("feat vector is"+vector);
        Document HotVector = new Document();
        for(String fixedColNames : colNames)
        	HotVector.append(fixedColNames, 0);
        
        Document featVec = Document.parse(vector);
        Set<String> keyset = featVec.keySet();
        //System.out.println("keyset in featvec documnt--"+featVec.keySet());
        for(String key : keyset){
            //System.out.println("contain key  "+key+" "+colNames.contains(key));
            if(colNames.contains(key)){ 
                HotVector.replace(key, Integer.parseInt((String) featVec.get(key)));
            }
            //System.out.println("contain key  "+key+"_"+featVec.get(key)+" "+(colNames.contains(key+"_"+featVec.get(key))));
            if (colNames.contains(key+"_"+featVec.get(key))){
                HotVector.replace(key+"_"+featVec.getString(key), 1);    
            }
        }
        //System.out.println("remaining colnames "+colNames);
       
        //System.out.println("final keyset is "+HotVector.keySet());
        HotVectorList.add(HotVector.toJson());
    }
    return HotVectorList;
}


static ArrayList<String> makeFeatureVector(Document globalDict, Document record, List<String> initialList, LocalDate DateStr){
    
	Document samplefeatDict = new Document();
    ArrayList<String> docList = new ArrayList<>();
    String uid = record.getString("uid"); 
    for(String initial : initialList)
        samplefeatDict = (Document) samplefeatDict.append(initial, record.getString(initial));  // Insert INITIAL values as it is
    
    //samplefeatDict.append("Key1", uid);
    //System.out.println("Date"+DateStr.toString());
    //System.out.println("Inside Create Vector GlobalDict"+ globalDict);
    if(((Document)globalDict.get("UserID_"+uid)).keySet().contains(DateStr.toString()))
    {
    	HashMap<String, String> Features = new HashMap<>();
    	Document docAll = ((Document)((Document)globalDict.get("UserID_"+uid)).get("All"));
    	Document docDate = ((Document)((Document)globalDict.get("UserID_"+uid)).get(DateStr.toString()));
    	for(String key : docAll.keySet())
    	{
    		//System.out.println("Key= "+key);
    		// For Parent/Target key
    		if(key.equals("UserEventBased"))  
    		{
    			//all features from All UserEvent
    		     for (Cell<String, String, HashMap<String, String>> cell : metafile2.entityMap.getOrDefault("All".concat(key), HashBasedTable.create()).cellSet()) 
    		     {
    		    	 //System.out.println("Inside Func*****4"+" Key="+key+" cell.getRowKey()="+cell.getRowKey());
    		    	 //System.out.println("(" + cell.getRowKey() + "," + cell.getColumnKey() + ")=" + cell.getValue());
    		    	 Features = featfunc.FeatureFromAll((Document)((Document)docAll.get(key)).get(cell.getRowKey()),cell.getColumnKey(),cell.getRowKey(),cell.getValue(),DateStr);
    		    	 if(! Features.isEmpty())
    		    		 samplefeatDict.putAll(Features);
    		    	 //System.out.println("SampleDict1="+samplefeatDict);
    		     }
    		     //System.out.println("Inside Func*****2");
    		   //window features from date UserEvent
    		     for (Cell<String, String, HashMap<String, String>> cell : metafile2.entityMap.getOrDefault("Date".concat(key), HashBasedTable.create()).cellSet()) 
    		     {
    		    	 //System.out.println("Inside Func*****3"+" Key="+key+" cell.getRowKey()="+cell.getRowKey());
    		     //System.out.println("(" + cell.getRowKey() + "," + cell.getColumnKey() + ")=" + cell.getValue());
    		    	 if(cell.getColumnKey().equals("NA")) // Inner Key
    		    	 {
    		    		 //System.out.println("Dict="+docDate);
    		    		 Features = featfunc.FeatureFromWindow((Document)((Document)docDate.get(key)).get(cell.getRowKey()),cell.getRowKey(),cell.getValue());  // Inner is not the Document.
    		    		 //System.out.println("SampleDict="+samplefeatDict);
    		    	 }
    		    	else
    		    	 {
    		    		 if ((((Document)docDate.get(key)).get(cell.getRowKey())!=null) && (((Document)docDate.get(key)).get(cell.getColumnKey())!=null)) // check for NULL dictionary  //getCoulmnKey= INNER Key
    		    		 {
    		    			 Document innerDocument = (Document)((Document)((Document)docDate.get(key)).get(cell.getRowKey())).get(cell.getColumnKey());
    		    			 Features = featfunc.FeatureFromWindow(innerDocument,cell.getRowKey()+"_"+cell.getColumnKey(),cell.getValue());
    		     }}
    		    	 if(! Features.isEmpty())
    		    		 samplefeatDict.putAll(Features);
    		    	 //System.out.println("SampleDict2="+samplefeatDict);
    		     }
    		     
    		}
    		// For other than Target key
    		else
    		{
    			if(! metafile2.entityMap.getOrDefault("All".concat(key), HashBasedTable.create()).isEmpty())   // for PAKDD --> it is EMPTY
    		       {
    				   //System.out.println("Inside Func*****3");
    		    	   Set<Map.Entry<String,Object>>   DictSet = ((Document)docAll.get(key)).entrySet();    // Set of Product ids.
    		           for( Map.Entry<String, Object> entry : DictSet)  //entry is a single product in ALL
    		           {
    		        	   HashMap<String, Object> ProductFeat = new HashMap<String, Object>();
    		               for (Cell<String, String, HashMap<String, String>> cell : metafile2.entityMap.getOrDefault("All".concat(key), HashBasedTable.create()).cellSet()) 
    		               {
    		            	   Features = featfunc.FeatureFromAll((Document)entry.getValue(),cell.getColumnKey(),cell.getRowKey(),cell.getValue(),DateStr);
    		            	   if(! Features.isEmpty())
    		            		   samplefeatDict.putAll(Features);
    		            	   //System.out.println("SampleDict3="+samplefeatDict);
    		               }
    		               
    		               if (((Document)docDate.get(key)).keySet().contains(entry.getKey()))
    		               {
    		            	   for (Cell<String, String, HashMap<String, String>> cell : metafile2.entityMap.getOrDefault("Date".concat(key), HashBasedTable.create()).cellSet()) 
    		            	   {
    		            		   //System.out.println("(" + cell.getRowKey() + "," + cell.getColumnKey() + ")=" + cell.getValue());
    		            		   if(cell.getColumnKey().equals("NA"))
    		            			   Features = featfunc.FeatureFromWindow((Document)((Document)docDate.get(key)).get(entry.getKey()),cell.getRowKey(),cell.getValue());
    		            		   else
    		            		   {
    		            			   if ((((Document)docDate.get(key)).get(entry.getKey())!=null) && (((Document)docDate.get(key)).get(cell.getColumnKey())!=null))
    		            			   {
    		            				   Document innerDocument = (Document)((Document)((Document)docDate.get(key)).get(entry.getKey())).get(cell.getColumnKey());
    		            				   Features = featfunc.FeatureFromWindow(innerDocument,cell.getRowKey()+"_"+cell.getColumnKey(),cell.getValue());
    		            			   }
    		            		   }
    		            		   if(! Features.isEmpty())
    		            			   samplefeatDict.putAll(Features);
    		            		   //System.out.println("SampleDict4="+samplefeatDict);
    		            	   }
    		     
    		               }
    		               //ProductFeat.put("Label", createLabelINSTACART( entry.getKey(), record));    // Per product Entry.
    		               //samplefeatDict.putAll(ProductFeat);
    		               docList.add(new Document(samplefeatDict).toJson());	
    		               //System.out.println("DocList="+docList);
    		           }
    		        }
    		}
    		if(docAll.keySet().size()==1) // For PAKDD sampleFeatDict storing.
    		{
    			
    			//samplefeatDict.append("Label", createLabelPAKDD(record));
    			//System.out.println("Final Feature Dict="+samplefeatDict.toJson());
	            docList.add(samplefeatDict.toJson());
    		}
    	}
  }
    else
        System.out.println("Date not Found!!");
return  docList;
}
}
