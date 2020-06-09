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
 * This Class implements Runnable interface to create User dictionary from
 * Impression map for every USerID and store it in Ignite Cache.
 */
public class ImpRunnable_Enseble implements Runnable {

	public String uid;

	public ImpRunnable_Enseble(String uid) {
		// TODO Auto-generated constructor stub
		this.uid = uid;
	}

	@Override
	public void run() {
		LinkedList<String> listDoc = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.impMap.get(uid);
		Iterator iter = listDoc.iterator();
		String str = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.XGBOOSTCache.get(uid);
		Document globalDictWithoutUid; // Not including User_ID + uid
		Document globalDict = new Document();
		Document readUserDFN = null;
		Document readUserDF;
		Document record;
		String msg = "";
		String[] s = null;
		while (iter.hasNext()) {
			msg = (String) iter.next();
			s = msg.split(",");
			record = new Document();
			record.append("impression_id", s[4]);
			record.append("server_time", s[0]);
			record.append("uid", s[3]);
			record.append("platform", s[21]);
			record.append("inventory_type", s[15]);
			record.append("app_code", s[14]);
			record.append("os_version", s[20]);
			record.append("model", s[18]);
			record.append("network", s[19]);
			record.append("is_click", s[16]);
			record.append("is_conversion", s[17]);
			// Document record = (Document)iter.next();
			// System.out.println(Thread.currentThread().getId()+" "+record);
			if (str != null) {
				globalDictWithoutUid = new Document().parse(str);
				globalDict = new Document().append("UserID_" + uid, globalDictWithoutUid);
				readUserDF = (Document) globalDict.get("UserID_" + uid);
				readUserDFN = map_funcImp(readUserDF, record);
				globalDict.append("UserID_" + uid, readUserDFN);
				// tradeStreamingCfg.put(uid_key,readUserDFN); // inserting user
				// Dict
			} else {
				ArrayList<Date> dateWindow = new ArrayList<Date>();
				ArrayList<String> strDateWindow = new ArrayList<String>();
				String dateStr = "";
				Calendar cal = Calendar.getInstance();
				String impression_id = record.getString("impression_id");
				String server_time = record.getString("server_time");
				String uid = record.getString("uid");
				String platform = record.getString("platform");
				String inventory_type = record.getString("inventory_type");
				String app_code = record.getString("app_code");
				String os_version = record.getString("os_version");
				String model = record.getString("model");
				String network = record.getString("network");
				int is_click = record.getInteger("is_click");
				int is_conversion = record.getInteger("is_conversion");

				globalDict = new Document().append("UserID_" + uid,
						PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.AllDictionary());
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
				java.util.Date start_date = null;
				java.util.Date dateValue = null;
				long diffFromEndD = 0L;
				Date date = null;
				try {
					start_date = df.parse(server_time);
					// System.out.println(df2.format(start_date));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					dateValue = df2.parse(df2.format(start_date));
					// System.out.println(dateValue);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				for (int i = 0; i < 46; i++) {
					cal.setTime(dateValue);
					cal.add(cal.DATE, i);
					dateWindow.add(cal.getTime());
					// Converting date window into string window
					strDateWindow.add(cal.getTime().toString());
					dateStr = df2.format(cal.getTime());
					// System.out.println(dateStr);
					if (!((Document) globalDict.get("UserID_" + uid)).containsKey(dateStr)) {
						// Initializing users time dictionary
						((Document) globalDict.get("UserID_" + uid)).append(dateStr,
								PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windowDictionary());
					}

					// Calculating date difference and its valid window based
					// features
					diffFromEndD = TimeUnit.DAYS.convert(((cal.getTime()).getTime() - dateValue.getTime()),
							TimeUnit.MILLISECONDS) + 1;
					// Fetching doc into temp document for one DateStr
					Document tempDoc = (Document) ((Document) globalDict.get("UserID_" + uid)).get(dateStr);
					for (Long window : PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows) {
						// Long window = x;
						if (diffFromEndD <= window && diffFromEndD >= 0) {
							int win_index = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.indexOf(window);
							for (int j = win_index; j < PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.size(); j++) {
								// View/Order Counts
								tempDoc.replace(
										"imp_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j)
												+ "days",
										(int) tempDoc.get("imp_Count_Window"
												+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
								// Click Counts
								if (is_click == 1)
									tempDoc.replace(
											"imp_Count_Click_Window"
													+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",
											(int) tempDoc.get("imp_Count_Click_Window"
													+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days")
													+ 1);
								// Conversion Counts
								if (is_conversion == 1)
									tempDoc.replace(
											"imp_Count_Conversion_Window"
													+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",
											(int) tempDoc.get("imp_Count_Conversion_Window"
													+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days")
													+ 1);
							}
							break;
						}
					}
					((Document) globalDict.get("UserID_" + uid)).replace(dateStr, tempDoc);
				}

				// Operation on 'All' fields
				Document AllTempDoc = ((Document) ((Document) globalDict.get("UserID_" + uid)).get("All"));

				// Never Clicked
				if (is_click == 1)
					AllTempDoc.replace("imp_NeverClicked", 0);
				// Never Converted
				if (is_conversion == 1)
					AllTempDoc.replace("imp_NeverConverted", 0);
				// Assigning weekday
				cal.setTime(dateValue);
				int weekno = cal.get(cal.DAY_OF_WEEK);
				if (weekno <= 4) {
					AllTempDoc.replace("imp_Count_Weekday", AllTempDoc.getInteger("imp_Count_Weekday") + 1);
				}
				// Assigning hour
				int hourno = cal.get(cal.HOUR_OF_DAY);
				if (!((Document) AllTempDoc.get("imp_Count_Hour")).containsKey("Hour_" + hourno)) {
					((Document) AllTempDoc.get("imp_Count_Hour")).append("Hour_" + hourno, 1);
				} else {
					((Document) AllTempDoc.get("imp_Count_Hour")).replace("Hour_" + hourno,
							((Document) AllTempDoc.get("imp_Count_Hour")).getInteger("Hour_" + hourno) + 1);
				}
				// AppCode
				String appCodeValue = app_code;
				if (!((Document) AllTempDoc.get("imp_Count_AppCode")).containsKey("AppCode_" + appCodeValue)) {
					((Document) AllTempDoc.get("imp_Count_AppCode")).append("AppCode_" + appCodeValue, 1);
				} else {
					((Document) AllTempDoc.get("imp_Count_AppCode")).replace("AppCode_" + appCodeValue,
							((Document) AllTempDoc.get("imp_Count_AppCode")).getInteger("AppCode_" + appCodeValue) + 1);
				}
				// Network
				String networkValue = network;
				if (!((Document) AllTempDoc.get("imp_Count_Network")).containsKey("Network_" + networkValue)) {
					((Document) AllTempDoc.get("imp_Count_Network")).append("Network_" + networkValue, 1);
				} else {
					((Document) AllTempDoc.get("imp_Count_Network")).replace("Network_" + networkValue,
							((Document) AllTempDoc.get("imp_Count_Network")).getInteger("Network_" + networkValue) + 1);
				}
				// osVersion
				String osVersionValue = os_version;
				if (!((Document) AllTempDoc.get("imp_Count_OsVersion")).containsKey("OsVersion_" + osVersionValue)) {
					((Document) AllTempDoc.get("imp_Count_OsVersion")).append("OsVersion_" + osVersionValue, 1);
				} else {
					((Document) AllTempDoc.get("imp_Count_OsVersion")).replace("OsVersion_" + osVersionValue,
							((Document) AllTempDoc.get("imp_Count_OsVersion")).getInteger("OsVersion_" + osVersionValue)
									+ 1);
				}

				// Replace ALL in main GlobalDict
				((Document) globalDict.get("UserID_" + uid)).replace("All", AllTempDoc);
				// insert into Ignite Cache
				// tradeStreamingCfg.put(uid_key,globalDict);
			}

		}
		PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.XGBOOSTStreamer.addData(uid,
				((Document) globalDict.get("UserID_" + uid)).toJson());
		// PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.impMap.remove(uid);
		// System.out.println(globalDict);

	}

	/**
	 * This method is called internally from class ImpRunnable_Enseble which
	 * takes the particular users' feature dictionary and incoming message as
	 * argument and returns the updated feature dictionary.
	 * 
	 * @param globalDict
	 *            User's current state of feature dictionary
	 * @param record
	 *            takes the real-time messages that has come from User's portal.
	 * @return returns the updated feature dictionary.
	 */
	public Document map_funcImp(Document globalDict, Document record) {

		ArrayList<Date> dateWindow = new ArrayList<Date>();
		ArrayList<String> strDateWindow = new ArrayList<String>();
		String dateStr = "";
		Calendar cal = Calendar.getInstance();
		String impression_id = record.getString("impression_id");
		String server_time = record.getString("server_time");
		String uid = record.getString("uid");
		String platform = record.getString("platform");
		String inventory_type = record.getString("inventory_type");
		String app_code = record.getString("app_code");
		String os_version = record.getString("os_version");
		String model = record.getString("model");
		String network = record.getString("network");
		int is_click = record.getInteger("is_click");
		int is_conversion = record.getInteger("is_conversion");

		globalDict = new Document().append("UserID_" + uid, PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.AllDictionary());
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date start_date = null;
		java.util.Date dateValue = null;
		long diffFromEndD = 0L;
		Date date = null;
		try {
			start_date = df.parse(server_time);
			// System.out.println(df2.format(start_date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			dateValue = df2.parse(df2.format(start_date));
			// System.out.println(dateValue);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < 46; i++) {
			cal.setTime(dateValue);
			cal.add(cal.DATE, i);
			dateWindow.add(cal.getTime());
			// Converting date window into string window
			strDateWindow.add(cal.getTime().toString());
			dateStr = df2.format(cal.getTime());
			// System.out.println(dateStr);
			if (!globalDict.containsKey(dateStr)) {
				// Initializing users time dictionary
				globalDict.append(dateStr, PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windowDictionary());
			}

			// Calculating date difference and its valid window based features
			diffFromEndD = TimeUnit.DAYS.convert(((cal.getTime()).getTime() - dateValue.getTime()),
					TimeUnit.MILLISECONDS) + 1;
			// Fetching doc into temp document for one DateStr
			Document tempDoc = (Document) globalDict.get(dateStr);
			for (Long window : PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows) {
				// Long window = x;
				if (diffFromEndD <= window && diffFromEndD >= 0) {
					int win_index = PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.indexOf(window);
					for (int j = win_index; j < PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.size(); j++) {
						// View/Order Counts
						tempDoc.replace(
								"imp_Count_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days",
								(int) tempDoc.get("imp_Count_Window"
										+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
						// Click Counts
						if (is_click == 1)
							tempDoc.replace(
									"imp_Count_Click_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j)
											+ "days",
									(int) tempDoc.get("imp_Count_Click_Window"
											+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
						// Conversion Counts
						if (is_conversion == 1)
							tempDoc.replace(
									"imp_Count_Conversion_Window" + PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j)
											+ "days",
									(int) tempDoc.get("imp_Count_Conversion_Window"
											+ PAKDDLSTMCDFHiddenStatesAPI0_Ensemble.windows.get(j) + "days") + 1);
					}
					break;
				}
			}
			((Document) globalDict.get("UserID_" + uid)).replace(dateStr, tempDoc);
		}

		// Operation on 'All' fields
		Document AllTempDoc = ((Document) ((Document) globalDict.get("UserID_" + uid)).get("All"));

		// Never Clicked
		if (is_click == 1)
			AllTempDoc.replace("imp_NeverClicked", 0);
		// Never Converted
		if (is_conversion == 1)
			AllTempDoc.replace("imp_NeverConverted", 0);
		// Assigning weekday
		cal.setTime(dateValue);
		int weekno = cal.get(cal.DAY_OF_WEEK);
		if (weekno <= 4) {
			AllTempDoc.replace("imp_Count_Weekday", AllTempDoc.getInteger("imp_Count_Weekday") + 1);
		}
		// Assigning hour
		int hourno = cal.get(cal.HOUR_OF_DAY);
		if (!AllTempDoc.containsKey("imp_Count_Hour")) {
			AllTempDoc.append("imp_Count_Hour", new Document());
			((Document) AllTempDoc.get("imp_Count_Hour")).append("Hour_" + hourno, 1);
		} else {
			if (!((Document) AllTempDoc.get("imp_Count_Hour")).containsKey("Hour_" + hourno)) {
				((Document) AllTempDoc.get("imp_Count_Hour")).append("Hour_" + hourno, 1);
			} else {
				((Document) AllTempDoc.get("imp_Count_Hour")).replace("Hour_" + hourno,
						((Document) AllTempDoc.get("imp_Count_Hour")).getInteger("Hour_" + hourno) + 1);
			}
		}
		// AppCode
		String appCodeValue = app_code;
		if (!AllTempDoc.containsKey("imp_Count_AppCode")) {
			AllTempDoc.append("imp_Count_AppCode", new Document());
			((Document) AllTempDoc.get("imp_Count_AppCode")).append("AppCode_" + appCodeValue, 1);
		} else {
			if (!((Document) AllTempDoc.get("imp_Count_AppCode")).containsKey("AppCode_" + appCodeValue)) {
				((Document) AllTempDoc.get("imp_Count_AppCode")).append("AppCode_" + appCodeValue, 1);
			} else {
				((Document) AllTempDoc.get("imp_Count_AppCode")).replace("AppCode_" + appCodeValue,
						((Document) AllTempDoc.get("imp_Count_AppCode")).getInteger("AppCode_" + appCodeValue) + 1);
			}
		}
		// Network
		String networkValue = network;
		if (!AllTempDoc.containsKey("imp_Count_Network")) {
			AllTempDoc.append("imp_Count_Network", new Document());
			((Document) AllTempDoc.get("imp_Count_Network")).append("Network_" + networkValue, 1);
		} else {
			if (!((Document) AllTempDoc.get("imp_Count_Network")).containsKey("Network_" + networkValue)) {
				((Document) AllTempDoc.get("imp_Count_Network")).append("Network_" + networkValue, 1);
			} else {
				((Document) AllTempDoc.get("imp_Count_Network")).replace("Network_" + networkValue,
						((Document) AllTempDoc.get("imp_Count_Network")).getInteger("Network_" + networkValue) + 1);
			}
		}
		// osVersion
		String osVersionValue = os_version;
		if (!AllTempDoc.containsKey("imp_Count_OsVersion")) {
			AllTempDoc.append("imp_Count_OsVersion", new Document());
			((Document) AllTempDoc.get("imp_Count_OsVersion")).append("OsVersion_" + osVersionValue, 1);
		} else {
			if (!((Document) AllTempDoc.get("imp_Count_OsVersion")).containsKey("OsVersion_" + osVersionValue)) {
				((Document) AllTempDoc.get("imp_Count_OsVersion")).append("OsVersion_" + osVersionValue, 1);
			} else {
				((Document) AllTempDoc.get("imp_Count_OsVersion")).replace("OsVersion_" + osVersionValue,
						((Document) AllTempDoc.get("imp_Count_OsVersion")).getInteger("OsVersion_" + osVersionValue)
								+ 1);
			}
		}

		// Replace ALL in main GlobalDict
		globalDict.replace("All", AllTempDoc);

		for (String key : globalDict.keySet()) {
			if (key.equals("All") && key.equals("_id"))
				if (!strDateWindow.contains(key))
					globalDict.remove(key);
		}

		return globalDict;
	}

}
