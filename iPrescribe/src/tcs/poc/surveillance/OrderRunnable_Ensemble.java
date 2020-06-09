package tcs.poc.surveillance;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

/**
 * This Class implements Runnable interface to create User dictionary from Order map
 * for every USerID and store it in Ignite Cache.
 */
public class OrderRunnable_Ensemble implements Runnable {

	public String uid;
    public OrderRunnable_Ensemble(String uid) {
		// TODO Auto-generated constructor stub
    	this.uid=uid;
	}
	@Override
	public void run() {
		LinkedList<String> listDoc = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.orderMap.get(uid);
		Iterator iter = listDoc.iterator();
		String str = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.XGBOOSTCache.get(uid);
		Document globalDictWithoutUid; //Not including User_ID + uid
		Document globalDict = new Document();
		Document readUserDFN=null;
		Document readUserDF;
		Document record;
		String msg="";
		String[] s=null;
		while(iter.hasNext())
		{
			msg = (String)iter.next();
			s = msg.split(",");
			record = new Document();
			record.append("server_time", s[0]);
			record.append("device", s[1]);
			record.append("session_id", s[2]);
			record.append("uid", s[3]);
			record.append("order_id", s[4]);
			record.append("quantity", s[5]);
			record.append("price", s[6]);
			record.append("category1", s[7]);
			record.append("category2", s[8]);
			record.append("category3", s[9]);
			record.append("category4", s[10]);
			record.append("brand", s[11]);
			record.append("item_id", s[12]);
			//System.out.println(Thread.currentThread().getId()+"  "+record);
			if(str!=null)
    		{
				globalDictWithoutUid = new Document().parse(str);
				globalDict = new Document().append("UserID_" + uid, globalDictWithoutUid);
				readUserDF = (Document)globalDict.get("UserID_" + uid);
    			readUserDFN = map_funcOrder(readUserDF,record);
    			globalDict.append("UserID_" + uid,readUserDFN);
    			//tradeStreamingCfg.put(uid_key,readUserDFN); // inserting user Dict
    			
    		}
    		else
    		{
    			ArrayList<Date> dateWindow = new ArrayList<Date>();
            	ArrayList<String> strDateWindow = new ArrayList<String>();
            	String dateStr="";
            	Calendar cal = Calendar.getInstance();
    			String server_time=record.getString("server_time");
    			String uid=record.getString("uid");
    			String order_id = record.getString("order_id");
    			String device=record.getString("device");
    			String session_id=record.getString("session_id");
    			String price=record.getString("price");
    			String quantity=record.getString("quantity");
    			String category1=record.getString("category1");
    			String category2=record.getString("category2");
    			String category3=record.getString("category3");
    			String category4=record.getString("category4");
    			String brand=record.getString("brand");
    			String item_id=record.getString("item_id");
    			
    			globalDict = new Document().append("UserID_" + uid, PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.AllDictionary());
    			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    			DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
    			java.util.Date start_date = null;
    			java.util.Date dateValue = null;
    			long diffFromEndD = 0L;
    			Date date = null;
    			try {
    				start_date = df.parse(server_time);
    				//System.out.println(df2.format(start_date));
    			} catch (ParseException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    			try {
    				dateValue = df2.parse(df2.format(start_date));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			for(int i=0;i<46;i++)
    			{
    				cal.setTime(dateValue);
    				cal.add(cal.DATE, i);
    				dateWindow.add(cal.getTime());			            			
    				//Converting date window into string window
    				strDateWindow.add(cal.getTime().toString());
    				dateStr = df2.format(cal.getTime());
    				if(!((Document)globalDict.get("UserID_" + uid)).containsKey(dateStr))
    				{
    					// Initializing users time dictionary
    					((Document)globalDict.get("UserID_" + uid)).append(dateStr, PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windowDictionary());
    				}
    				
    				// Calculating date difference and its valid window based features
    				diffFromEndD = TimeUnit.DAYS.convert(((cal.getTime()).getTime() - dateValue.getTime()),TimeUnit.MILLISECONDS)+1;
    				// Fetching doc into temp document for one DateStr
    				Document tempDoc = (Document)((Document)globalDict.get("UserID_" + uid)).get(dateStr);
    				for(Long window:PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows)
    				{
    					//Long window = x;
    					if(diffFromEndD <= window && diffFromEndD>=0)
    					{
    						int win_index = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.indexOf(window);
    						for(int j=win_index;j<PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.size();j++)
    						{
    							// View/Order Counts
    							tempDoc.replace("order_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",(int)tempDoc.get("order_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
    							// Session Counts
    							tempDoc.replace("order_Count_Session_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",(int)tempDoc.get("order_Count_Session_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
    						}
    						break;
    					}
    				}
    				((Document)globalDict.get("UserID_" + uid)).replace(dateStr, tempDoc);
    			}
    			
    			// Operation on 'All' fields
    			Document AllTempDoc = ((Document)((Document)globalDict.get("UserID_" + uid)).get("All"));
    			// Assigning weekday
    			cal.setTime(dateValue);
    			int weekno = cal.get(cal.DAY_OF_WEEK);
    			if(weekno <= 4)
    			{
    				AllTempDoc.replace("order_Count_Weekday", AllTempDoc.getInteger("order_Count_Weekday")+1);
    			}
    			// Assigning hour
    			int hourno = cal.get(cal.HOUR_OF_DAY);
    			if(!((Document)AllTempDoc.get("order_Count_Hour")).containsKey("Hour_" + hourno))
    			{
    				((Document)AllTempDoc.get("order_Count_Hour")).append("Hour_" + hourno, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Hour")).replace("Hour_" + hourno, ((Document)AllTempDoc.get("order_Count_Hour")).getInteger("Hour_" + hourno)+1);
    			}
    			// Category 1
    			String cat1Value = category1;
    			if(!((Document)AllTempDoc.get("order_Count_Category1")).containsKey("Category1_" + cat1Value))
    			{
    				((Document)AllTempDoc.get("order_Count_Category1")).append("Category1_" + cat1Value, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Category1")).replace("Category1_" + cat1Value, ((Document)AllTempDoc.get("order_Count_Category1")).getInteger("Category1_" + cat1Value)+1);
    			}
    			// Category 2
    			String cat2Value = category2;
    			if(!((Document)AllTempDoc.get("order_Count_Category2")).containsKey("Category2_" + cat2Value))
    			{
    				((Document)AllTempDoc.get("order_Count_Category2")).append("Category2_" + cat2Value, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Category2")).replace("Category2_" + cat2Value, ((Document)AllTempDoc.get("order_Count_Category2")).getInteger("Category2_" + cat2Value)+1);
    			}
    			// Category 3
    			String cat3Value = category3;
    			if(!((Document)AllTempDoc.get("order_Count_Category3")).containsKey("Category3_" + cat3Value))
    			{
    				((Document)AllTempDoc.get("order_Count_Category3")).append("Category3_" + cat3Value, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Category3")).replace("Category3_" + cat3Value, ((Document)AllTempDoc.get("order_Count_Category3")).getInteger("Category3_" + cat3Value)+1);
    			}
    			// Category 4
    			String cat4Value = category4;
    			if(!((Document)AllTempDoc.get("order_Count_Category4")).containsKey("Category4_" + cat4Value))
    			{
    				((Document)AllTempDoc.get("order_Count_Category4")).append("Category4_" + cat4Value, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Category4")).replace("Category4_" + cat4Value, ((Document)AllTempDoc.get("order_Count_Category4")).getInteger("Category4_" + cat4Value)+1);
    			}
    			// Device
    			String deviceValue = device;
    			if(!((Document)AllTempDoc.get("order_Count_Device")).containsKey("Device_" + deviceValue))
    			{
    				((Document)AllTempDoc.get("order_Count_Device")).append("Device_" + deviceValue, 1);
    			}
    			else
    			{
    				((Document)AllTempDoc.get("order_Count_Device")).replace("Device_" + deviceValue, ((Document)AllTempDoc.get("order_Count_Device")).getInteger("Device_" + deviceValue)+1);
    			}
    			//Replace ALL in main GlobalDict
    			((Document)globalDict.get("UserID_" + uid)).replace("All", AllTempDoc);
    		}
			
		}
		PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.XGBOOSTStreamer.addData(uid,((Document)globalDict.get("UserID_" + uid)).toJson());
		//PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.orderMap.remove(uid);
		//System.out.println(globalDict);
		
	}
	
	/**
	 * This method is called internally from class OrderRunnable_Enseble which
	 * takes the particular users' feature dictionary and incoming message as
	 * argument and returns the updated feature dictionary.
	 * 
	 * @param globalDict
	 *            User's current state of feature dictionary
	 * @param record
	 *            takes the real-time messages that has come from User's portal.
	 * @return returns the updated feature dictionary.
	 */
	public Document map_funcOrder(Document globalDict, Document record) {
		ArrayList<Date> dateWindow = new ArrayList<Date>();
    	ArrayList<String> strDateWindow = new ArrayList<String>();
    	String dateStr="";
    	Calendar cal = Calendar.getInstance();
    	String server_time=record.getString("server_time");
		String uid=record.getString("uid");
		String order_id = record.getString("order_id");
		String device=record.getString("device");
		String session_id=record.getString("session_id");
		String price=record.getString("price");
		String quantity=record.getString("quantity");
		String category1=record.getString("category1");
		String category2=record.getString("category2");
		String category3=record.getString("category3");
		String category4=record.getString("category4");
		String brand=record.getString("brand");
		String item_id=record.getString("item_id");
		
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date start_date = null;
		java.util.Date dateValue = null;
		long diffFromEndD = 0L;
		Date date = null;
		try {
			start_date = df.parse(server_time);
			//System.out.println(df2.format(start_date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			dateValue = df2.parse(df2.format(start_date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i=0;i<46;i++)
		{
			cal.setTime(dateValue);
			cal.add(cal.DATE, i);
			dateWindow.add(cal.getTime());			            			
			//Converting date window into string window
			strDateWindow.add(cal.getTime().toString());
			dateStr = df2.format(cal.getTime());
			if(!globalDict.containsKey(dateStr))
			{
				// Initializing users time dictionary
				globalDict.append(dateStr, PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windowDictionary());
			}
			
			// Calculating date difference and its valid window based features
			diffFromEndD = TimeUnit.DAYS.convert(((cal.getTime()).getTime() - dateValue.getTime()),TimeUnit.MILLISECONDS)+1;
			// Fetching doc into temp document for one DateStr
			Document tempDoc = (Document)globalDict.get(dateStr);
			for(Long window:PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows)
			{
				//Long window = x;
				if(diffFromEndD <= window && diffFromEndD>=0)
				{
					int win_index = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.indexOf(window);
					for(int j=win_index;j<PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.size();j++)
					{
						// View/Order Counts
						tempDoc.replace("order_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",(int)tempDoc.get("order_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
						// Session Counts
						tempDoc.replace("order_Count_Session_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",(int)tempDoc.get("order_Count_Session_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
					}
					break;
				}
			}
			globalDict.replace(dateStr, tempDoc);
		}
		
		// Operation on 'All' fields
		Document AllTempDoc = ((Document)globalDict.get("All"));
		// Assigning weekday
		cal.setTime(dateValue);
		int weekno = cal.get(cal.DAY_OF_WEEK);
		if(weekno <= 4)
		{
			AllTempDoc.replace("order_Count_Weekday", AllTempDoc.getInteger("order_Count_Weekday")+1);
		}
		// Assigning hour
		int hourno = cal.get(cal.HOUR_OF_DAY);
		if(!AllTempDoc.containsKey("order_Count_Hour"))
		{
			AllTempDoc.append("order_Count_Hour", new Document());
			((Document)AllTempDoc.get("order_Count_Hour")).append("Hour_" + hourno, 1);
		}
		else
		{
			if(!((Document)AllTempDoc.get("order_Count_Hour")).containsKey("Hour_" + hourno))
			{
				((Document)AllTempDoc.get("order_Count_Hour")).append("Hour_" + hourno, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Hour")).replace("Hour_" + hourno, ((Document)AllTempDoc.get("order_Count_Hour")).getInteger("Hour_" + hourno)+1);
			}
		}
		// Category 1
		String cat1Value = category1;
		if(!AllTempDoc.containsKey("order_Count_Category1"))
		{
			AllTempDoc.append("order_Count_Category1", new Document());
			((Document)AllTempDoc.get("order_Count_Category1")).append("Category1_" + cat1Value, 1);
		}
		else
		{
			if(!((Document)AllTempDoc.get("order_Count_Category1")).containsKey("Category1_" + cat1Value))
			{
				((Document)AllTempDoc.get("order_Count_Category1")).append("Category1_" + cat1Value, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Category1")).replace("Category1_" + cat1Value, ((Document)AllTempDoc.get("order_Count_Category1")).getInteger("Category1_" + cat1Value)+1);
			}
		}
		// Category 2
		String cat2Value = category2;
		if(!AllTempDoc.containsKey("order_Count_Category2"))
		{
			AllTempDoc.append("order_Count_Category2", new Document());
			((Document)AllTempDoc.get("order_Count_Category2")).append("Category2_" + cat2Value, 1);
		}
		else
		{
			if(!((Document)AllTempDoc.get("order_Count_Category2")).containsKey("Category2_" + cat2Value))
			{
				((Document)AllTempDoc.get("order_Count_Category2")).append("Category2_" + cat2Value, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Category2")).replace("Category2_" + cat2Value, ((Document)AllTempDoc.get("order_Count_Category2")).getInteger("Category2_" + cat2Value)+1);
			}
		}
		// Category 3
		String cat3Value = category3;
		if(!AllTempDoc.containsKey("order_Count_Category3"))
		{
			AllTempDoc.append("order_Count_Category3", new Document());
			((Document)AllTempDoc.get("order_Count_Category3")).append("Category3_" + cat3Value, 1);
		}
		else
		{
			if(!((Document)AllTempDoc.get("order_Count_Category3")).containsKey("Category3_" + cat3Value))
			{
				((Document)AllTempDoc.get("order_Count_Category3")).append("Category3_" + cat3Value, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Category3")).replace("Category3_" + cat3Value, ((Document)AllTempDoc.get("order_Count_Category3")).getInteger("Category3_" + cat3Value)+1);
			}
		}
		// Category 4
		String cat4Value = category4;
		if(!AllTempDoc.containsKey("order_Count_Category4"))
		{
			AllTempDoc.append("order_Count_Category4", new Document());
			((Document)AllTempDoc.get("order_Count_Category4")).append("Category4_" + cat4Value, 1);
		}
		else
		{
			if(!((Document)AllTempDoc.get("order_Count_Category4")).containsKey("Category4_" + cat4Value))
			{
				((Document)AllTempDoc.get("order_Count_Category4")).append("Category4_" + cat4Value, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Category4")).replace("Category4_" + cat4Value, ((Document)AllTempDoc.get("order_Count_Category4")).getInteger("Category4_" + cat4Value)+1);
			}
		}
		// Device
		String deviceValue = device;
		if(!AllTempDoc.containsKey("order_Count_Device"))
		{
			AllTempDoc.append("order_Count_Device", new Document());
			((Document)AllTempDoc.get("order_Count_Device")).append("Device_" + deviceValue, 1);
		}
		else
		{	
			if(!((Document)AllTempDoc.get("order_Count_Device")).containsKey("Device_" + deviceValue))
			{
				((Document)AllTempDoc.get("order_Count_Device")).append("Device_" + deviceValue, 1);
			}
			else
			{
				((Document)AllTempDoc.get("order_Count_Device")).replace("Device_" + deviceValue, ((Document)AllTempDoc.get("order_Count_Device")).getInteger("Device_" + deviceValue)+1);
			}
		}
		//Replace ALL in main GlobalDict
				globalDict.replace("All", AllTempDoc);
				
				for(String key:globalDict.keySet())
				{
					if(key.equals("All") && key.equals("_id"))
						if(!strDateWindow.contains(key))
							globalDict.remove(key);
				}
				
				return globalDict;
	}
	

}
