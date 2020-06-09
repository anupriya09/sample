/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com;

import static com.XGBLSTMLoaderMain.dft;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.joda.time.Days;
import org.joda.time.LocalDate;

/**
 * This class contains methods used to calculate feature vector for model
 * training and inference.
 * 
 */
public class FeatureFuncV2 {
	/**
	 * This method is to find max_key from the list of certain features.
	 * @param document
	 * 			The document corresponding to OuterKey (3rd level in the feature dictionary).
	 * @return String
	 * 			Key having max value within the list.
	 */
public static String getMaxValuedKey(Object document) {
        String maxKey = null;
        if(document.getClass().toString().equals("class org.bson.Document")){
            int maxValue = Integer.MIN_VALUE;
            Set<String> keySet = ((Document)document).keySet();
            Iterator itr = keySet.iterator();
            while(itr.hasNext()){
                String key = (String) itr.next();
                if(maxValue < Integer.parseInt((String)((Document)document).get(key)))
                {  maxKey = key;
                    maxValue = Integer.parseInt((String)((Document)document).get(key));
            }}
        } 
        return maxKey;
    }
    

public static HashMap<String, String> FeatureFromWindow(Document sampleDict, String KeyName,HashMap<String, String> Row){
    
	HashMap<String, String> featureDict = new HashMap<>();
	if(sampleDict!=null)
     {
       //System.out.println("SampleDict="+ sampleDict);
       String functionList = Row.get("Function_List");
       for (String function : functionList.split(",")){
       Set<Map.Entry<String,Object>> sampleDictSet = sampleDict.entrySet();
       for(Map.Entry<String, Object> entry : sampleDictSet){
          if(! entry.getValue().getClass().toString().equals("class org.bson.Document"))
          {
           if(function.equals("direct"))
              featureDict.put(KeyName+"_"+entry.getKey(), (String)entry.getValue());
           if(function.equals("atleast")){
               for(int atleastItem : XGBLSTMLoaderMain.atleastThresolds){
               if((Integer.parseInt((String) entry.getValue())) >= atleastItem)
                   featureDict.put(KeyName + entry.getKey() + "_Atleast_" + Integer.toString(atleastItem), Integer.toString(1));
               else
                   featureDict.put(KeyName + entry.getKey() + "_Atleast_" + Integer.toString(atleastItem), Integer.toString(0));
               }
           }
         } 
       }
      }
//       System.out.println("featFuncEndWindow"+featureDict);
     }
     return featureDict;
    }

	/**
	 * Iterate over function-list.
	 * For each function
	 * 		If function is “direct”
	 * 			If inner-key value is document then add all keys of document to feature-vector
	 * 			Else, add inner-key value directly to feature-vector.
	 * 		If function is “lastOccuranceDays” Then calculate duration from last occurance of event and add to feature-vector
	 * 		If function is “max_key” Add the key having highest count to feature vector.
	 * 		If function is “average” Then calculate average over inner-key value by dividing with total count. Add it to feature-vector.
	 * @param sampleDict
	 * 			The document inside the Non-Temporal features.
	 * @param KeyName
	 * 			Name of CDF_Header.
	 * @param OuterKey
	 * 			Key at the 3rd level of feature dictionary.
	 * @param Row
	 * 			Entire row in metafile2 for corresponding CDF_Header.
	 * @param DateStr
	 * 			Corresponding data from feature dictionary.
	 * @return HashMap
	 * 			Map having features(key:value).
	 */
public static HashMap<String, String> FeatureFromAll(Document sampleDict, String KeyName, String OuterKey, HashMap<String, String> Row, LocalDate DateStr){
    HashMap<String, String> featureDict = new HashMap<String, String>();
      try {
          for(String function : Row.get("Function_List").split(",")){
              //System.out.println("in fea all"+KeyName+OuterKey+function+"  "+sampleDict);
            if(function.equals("direct"))
            { 	//System.out.println("direct");
                if(! sampleDict.get(KeyName).getClass().toString().equals("class org.bson.Document"))
                    featureDict.put(OuterKey+"_"+KeyName, (String)sampleDict.get(KeyName));
                else{ 
                    Set<Map.Entry<String,Object>> entryDictSet = ((Document)sampleDict.get(KeyName)).entrySet();
                    for(Map.Entry<String, Object> innerEntry : entryDictSet){ 
                        featureDict.put(OuterKey+"_"+KeyName+"_"+innerEntry.getKey(), (String)innerEntry.getValue());
                   }
                }
            }
                if(function.equalsIgnoreCase("lastOccuranceDays"))  
                { //System.out.println("lod");
                    int Duration = java.lang.Math.abs(Days.daysBetween(DateStr,dft.parseLocalDate((String)sampleDict.get(KeyName))).getDays());
                    featureDict.put(KeyName+"_Last_ordered_days", Integer.toString(Duration));
                      }
                if(function.equals("max_key")){ //System.out.println("maxKey");
                   featureDict.put(OuterKey+"_MaxKey_"+KeyName, getMaxValuedKey(sampleDict.get(KeyName)));
                   
                }
                if(function.equals("average")){
                    int total_count = Integer.parseInt((String)sampleDict.get("Count_on_weekdays")) + Integer.parseInt((String)sampleDict.get("Count_on_weekends"));

                    if(KeyName.contains("Sum")){
                        String newKey = KeyName.replaceAll("Sum", "Average");
                        featureDict.put(KeyName+"_"+newKey, Integer.toString(Integer.parseInt((String)sampleDict.get(KeyName))/total_count));
                    }
                }
          }
        } catch (Exception e) {
        }
        //System.out.println("featFuncEnd"+featureDict);
    return featureDict;
    }

}
