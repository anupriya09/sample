package tcs.poc.surveillance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

public class CDFRunnable2 implements Runnable {
	private String uid;
    
    public CDFRunnable2(String uid) {
		// TODO Auto-generated constructor stub
    	this.uid=uid;
    	
	}
    public void run() {
    	//System.out.println("Entered thread");
		LinkedList<String> listDoc = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.cdfMap.get(uid);
		String docs=PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.LSTMDictCache.get(uid);
		//Document dd=new Document().parse(PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.TestCache.get(uid));
		//System.out.println("got document");
		//System.out.println(docs);
		Document lstmData;
		if(docs==null) {
			lstmData=new Document();
		}else {
			lstmData=new Document().parse(docs);
		}
		
		//LinkedList<String> listDoc = cdfMap.get(uid);
		Iterator iter = listDoc.iterator();
		Document record;
		String row="";
		String currentLevel0=uid;
		String cdfColLev1 = "";
		String[] s=null;
		//Object[] confdicts=configDicts(PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.configread,PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.funcType);
		Map<String,ArrayList<String>> headFuncDict=PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.headFuncDict;
		Map<String,String> typedict=PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.typedict;
		while(iter.hasNext())
		{
			row = (String)iter.next();
			s = row.split(",",25);
			record = new Document();
			int eventNumber = 0;
			int userChangeFlag = 0;
			//System.out.println("entered");
			

			record.append("app_code", s[0]);
			record.append("brand", s[1]);
			record.append("category1", s[2]);
			record.append("category2", s[3]);
			record.append("category3", s[4]);
			record.append("category4", s[5]);
			record.append("device", s[6]);
			record.append("dow", s[7]);
			record.append("eval_set", s[8]);
			record.append("hour_of_day", s[9]);
			record.append("inventory_type", s[10]);
			record.append("is_click", s[11]);
			record.append("is_conversion", s[12]);
			record.append("item_id", s[13]);
			record.append("model", s[14]);
			record.append("network", s[15]);
			record.append("os_version", s[16]);
			record.append("platform", s[17]);
			record.append("price", s[18]);
			record.append("quantity", s[19]);
			record.append("server_time", s[20]);
			record.append("session_id", s[21]);
			record.append("transaction_id", s[22]);
			record.append("transaction_type", s[23]);
			record.append("uid", s[24]);
			
			for (Map.Entry<String,ArrayList<String>> entry : headFuncDict.entrySet()) {
				//System.out.println("entered2");
				ArrayList<String> functionList=entry.getValue();
				for(String func:functionList) {
					//System.out.println("entered3");
					String fillerValue="";
					if(func.equals("TypeN")){
						//System.out.println("before"+lstmData);
						lstmData=directfromDF(lstmData, currentLevel0, cdfColLev1, record.get(entry.getKey()).toString(), entry.getKey(), eventNumber, fillerValue,PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.catHeaderList,true,typedict);
						//System.out.println("after"+lstmData);
					}
					if(func.equals("TypeO")){
						////System.out.println()
						lstmData=transactionType(lstmData, currentLevel0, cdfColLev1,record.get(entry.getKey()).toString(),record.getString("is_click").toString(),entry.getKey(), eventNumber);
					}
			    ////System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
				}
			}//System.out.println(lstmData); 
			//System.out.println("ytrainthread"+(String) record.get("is_conversion"));
//			if(record.get("is_conversion").toString().length()>0) {
//				PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.ytrain.add((String) record.get("is_conversion"));
//			}
			PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.users.add(uid);
			
		}
		//System.out.println(lstmData);
		PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.LSTMDictCache.put(uid, lstmData.toJson());
		Document d=new Document().parse(PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.LSTMDictCache.get(uid));
	}

    public static Document transactionType(Document lstmData, String colLev0Value, String colLev1Value, String columnValue, String y,String header, int eventNumber) {
		String putvalue="";
		if(columnValue.equals("impression")) {
			putvalue=y;
		}else {
			putvalue="-1";
		}
		Set<String> users=lstmData.keySet();
    	String functionName="direct";
    	ArrayList<String> features=new ArrayList<>();
    	features.add(putvalue);
    	if(!users.contains(colLev0Value)) {
    		Document user = new Document();
    		user.put("isImpression",features);
    		lstmData.put(colLev0Value,user);

    	}else {
    		Document user=(Document)lstmData.get(colLev0Value);
    		if(!user.containsKey("isImpression")) {
    			user.put("isImpression", features);
    		}else {
    			ArrayList<String> alreadyfeatures=(ArrayList<String>)user.get("isImpression");
    			
    			alreadyfeatures.add(putvalue);
    			user.replace("isImpression", alreadyfeatures);
    			lstmData.replace(colLev0Value,user);
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
    
    public static Document directfromDFold(Document lstmData, String colLev0Value, String colLev1Value, String columnValue, String header,int eventNumber,String fillerValue,ArrayList<String> catHeaderList,boolean istrain,Map<String, String> typelist) {
    	Set<String> users=lstmData.keySet();
    	//System.out.println("putting1");
    	String functionName="direct";
    	if(columnValue.length()==0){
    		if(typelist.get(header).equals("continuous")){
    			columnValue="0";
    		}
    	}
    	//System.out.println("putting2");
    	//System.out.println(catHeaderList);
    	if(catHeaderList.size()>0 && (catHeaderList.contains(header))) {
            columnValue=toCodesDirect2(columnValue);
    	}
    	//System.out.println("putting3");
    	ArrayList<String> features=new ArrayList<>();
    	features.add(columnValue);
    	//System.out.println("putting");
    	if(!users.contains(colLev0Value)) {
    		Document user = new Document();
    		user.put(header + '_' + functionName,features);
    		lstmData.put(colLev0Value,user);

    	}else {
    		Document user=(Document)lstmData.get(colLev0Value);
    		if(!user.containsKey(header + '_' + functionName)) {
    			user.put(header + '_' + functionName, features);
    		}else {
    			ArrayList<String> alreadyfeatures=(ArrayList<String>)user.get(header + '_' + functionName);
    			alreadyfeatures.add(columnValue);
    			user.replace(header + '_' + functionName, alreadyfeatures);
    			lstmData.replace(colLev0Value,user);
    		}
    			
    	}
    	return lstmData;
	}
	public static String toCodesDirect2(String value) {
		Character[] fullsetarr= {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9','.'};
		ArrayList<Character> fullset  = new ArrayList<Character>(Arrays.asList(fullsetarr));
		String code="";
		//System.out.println("putting4");
		for(int i=0;i<value.length();i++) {
			char c=value.charAt(i);
			if(Character.isDigit(c)) {
				code+=c;
			}else {
				int ind=fullset.indexOf(c)+10;
				code+=Integer.toString(ind);
			}
			
			
		}
		//System.out.println("putting5");
		if(code.equals("")){
			return "0.0";
		}
		return code;
	}

}

