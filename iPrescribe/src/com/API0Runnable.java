package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static com.XGBLSTMLoaderMain.dfInCDF;
import static com.XGBLSTMLoaderMain.dft;

//import org.apache.spark.sql.Row;
import org.bson.Document;
import org.joda.time.LocalDate;

import com.sun.research.ws.wadl.Doc;

/**
 * This class implements a runnable interface for API0 in the generalized form
 * It is able to generate the features for both PAKDD and INSTACART challenge.
 */
public class API0Runnable implements Runnable {

	private String uid;
	/**
     * @param uid  changessssssss
     * 		User id for which the feature record is being generated and stored 
     * 		into Ignite cache.
     */
    public API0Runnable(String uid) {
		// TODO Auto-generated constructor stub
    	this.uid=uid;
	}
	@Override
	public void run() {
		LinkedList<Document> listDoc = XGBLSTMLoaderMain.recordMap.get(uid);
		Iterator iter = listDoc.iterator();
		
		/**
		 * Fetch from ignite cache or create the new user document.
		 */
		String str = XGBLSTMLoaderMain.XGBCache.get(uid);     
		//String str = null;
		Document globalDict; 
		if(str!=null)
		{
			globalDict = new Document().parse(str);            
		}
		else
		{
			 //If user is new and have no existing GlobalDictionary in cache
			globalDict = new Document().append("UserID_"+uid, XGBLSTMLoaderMain.dict.Dictionary());   // **** Main part in Generalization
		}
    //**********************************************************************************
		Document record ;
		while(iter.hasNext())
		{    //processing one row of CDF
			record = (Document)iter.next();
			LocalDate server_time = null;
	        try {
	                server_time = XGBLSTMLoaderMain.dfInCDF.parseLocalDate(record.getString("server_time"));
	            }catch (Exception e) {
	                e.printStackTrace();
	            }
	        
	        /**
	         * columns is list of headers from CDF file (Same will be there in metafile1 header column)
	         */
		 ArrayList<String> columns = new ArrayList<>(XGBLSTMLoaderMain.metafile1.getIndexColumnForAllRow()); // 
		 long startRow =System.currentTimeMillis();
		 for(String col : columns) {  
		  //processing each column of CDF row
			 //System.out.println("Column: "+col);
		   if(col.equals("windows")) continue;                //ignore if column name is windows
		   
	            String col_value;
	            try{
	             col_value = record.getString(col);
	            }catch(java.lang.Exception e){
	                col_value = "NA";  System.out.println(e);
	            }
	           
	            /**
		         * Get dependency list from metafile1 of the column
		         */
	            String dependencyColValue = XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col,"Dependency");
	               
	            if ( (dependencyColValue != null) && (!dependencyColValue.isEmpty()) && !col_value.equals("NA")){ 
				       //if dependency list is not empty and column value is not "NA"
	                    String[] DependencyList = dependencyColValue.split("/");        //Get all dependency lists by spliting       
	                    ArrayList<String> depList = new ArrayList<>();
	                    for( String dependency : DependencyList)
	                    {		
	                            String[] dependencylist = dependency.split(",");
	                            depList.add(dependencylist[dependencylist.length-1]);   //Take last element of each dependency list
	                    }									
	                 
	                   if ((XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col,"Type")).equals("date") && ! (XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function") == null))
	                   { 
	                    //If column value is date-type and function is not null
						for ( String function : XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function").split(",")){
							//traverse over function list of the column given in metafile1
	                        if (function.equals("sum_reoccurance_duration")){  
	                             globalDict = SumReoccuranceDuration.func(depList, globalDict, record, server_time);    // Generalization is Done**
	                         }
	                         if (function.equals("occurance_in_week")){ 
	                             globalDict = OccuranceInWeek.func(depList, globalDict, record, server_time);			// Generalization is Done**
	                         }
	                      }}
						  
						  
	                   else if ((XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col,"Type")).equals("categorical") && ! (XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function") == null)){ 
	                    //if column value is of categorical-type and function is not null
						for ( String function : XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function").split(",")){                
	                        function = function.replaceAll("\\s", "");
	                        if (function.equals("dictionary")){ 
							//insert column value into column-name dictionary
	                            globalDict = Dictionary.func(depList, globalDict,record, col);  // Generalization is Done**
	                        }
	                        if (function.equals("length")){ 
							 //aggregate number of the elements in column-value 
	                            globalDict = Length.func(depList, globalDict,record, col);     	// Generalization is Done**
	                        }
	                        if (function.equals("direct")){ 
							 //insert column-value directly in globalDictionary
	                            globalDict = Direct.func(depList, globalDict,record, col);		// Generalization is Done**
	                        }
	                        if (function.equals("sumOf")){ 
							    //aggegate all the values of the column
	                            globalDict = SumOf.func(depList, globalDict,record, col);		// Generalization is Done**
	                        }
	                        if (function.equals("windows")){  
							  //apply windowing funtion on all the column's values
	                            globalDict = CategoricalWindow.func(depList, globalDict,record, server_time, col);   // Generalization is Done**
	                        }
	                        if (function.equals("CreateHotVector")){  
	                             if(col_value.equals("train")){         //generate feature vector
								// fetch initial list from metafile1	 
	                             List<String> initialList = Arrays.asList( XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Initials").split(","));
	                             try { 
	                            	 CreateHotVectorV2.func(globalDict, record, depList, initialList , server_time);  //creating hot vector
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
	                           }
	                         }

	                    }}
	                   else if ((XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col,"Type")).equals("boolean") && ! (XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function") == null)){ 
	                      //if column-value is of boolean-type and function is not null
						  for ( String function : XGBLSTMLoaderMain.metafile1.getCellValueForParticularRow(col, "Function").split(",")){
	                          function = function.replaceAll("\\s", "");
	                          //System.out.println("Inside BOOLEAN condistion"+function);
	                          if (function.equals("NeverOccured")){     
							  //check whether the boolean action ever happened or not
	                             globalDict = NeverOccured.func(depList, globalDict,record, col);        // Generalization is Done**
	                        }
	                          if (function.equals("windows")){ 
							  //apply windowing function on boolean value (ex is_click,reordered list etc)        // Generalization is Done**
	                              globalDict = BooleanWindow.func(depList, globalDict,record, server_time, col);  //BoooleanWindow is nothing but window
	                         }
	                    }}
	                   //System.out.println("Temp***"+globalDict.toJson());
	            }}
		 			//System.out.println("Temp***"+globalDict.toJson());
				}
		//System.out.println("gggggg*"+globalDict.toJson());
		/**
         * Add user dictionary into Ignite cache.
         */
		XGBLSTMLoaderMain.XGBStreamer.addData(uid,globalDict.toJson());
		
	}
	
}
