package com;
import java.io.*;
import java.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.List;

/**
 * Create a hashmap(string, hashmap(string, string)), read the first line (headers) of metafile1.
 * Values in column “CDF_header” in metafile1 are made index key of hashmap
 * For example, (category1,cdf_cat=product_only, dependency=uid,transcation_type, type=categorical,function=dictionary)
 */
public class PreProcessCSV{

	HashMap<String, HashMap<String, String>> dictCSV;
	ArrayList<String> elementIndeX;

	/**
	 * @param filename
	 * 			Metafile1 is parsed and put into (LinkedHashMap(String, HashMap(String, String))
	 * 			Outer key is CDF_Header and inner hash-map is for column header and its value in 
	 * 			metafile1.
	 */
	PreProcessCSV(String filename){
            
		 BufferedReader br = null;
        
        int headerIndex = 0;
        dictCSV = new LinkedHashMap<String, HashMap<String, String>>();
        elementIndeX = new ArrayList<String>();
        try {
            br = new BufferedReader(new FileReader(filename));
            String line;
            //int line1 = 0;
            int isfirstLineOfCSVFile = 0;
            while ((line = br.readLine()) != null) {

            
            	List<String> commaSeparated = Arrays.asList(line.split(","));
				if(isfirstLineOfCSVFile == 0){

					ListIterator<String> itr=commaSeparated.listIterator();    
					while(itr.hasNext()){  
						String element = itr.next();
						if(element.equals("Header")){
							continue;
						}
						elementIndeX.add(element);
					}  	

					headerIndex = commaSeparated.indexOf("Header");
					isfirstLineOfCSVFile++;
				}	

				else{
					String key = commaSeparated.get(headerIndex);   //header columns is set as key for hash-map
					String value = "";
					HashMap<String, String> valueMap = new HashMap<String, String>();

					ListIterator<String> itr=commaSeparated.listIterator();  // metafile 1
					int start = 0;
					int valueIndex=0;
					String multiValue="";
					int startMultiValue=0;
					while(itr.hasNext()){  
						String element = itr.next();
						
						if(valueIndex==headerIndex){
							headerIndex--;
							continue;
						}
						if(valueIndex!=headerIndex){
							if(startMultiValue==1){
								if(element.endsWith("\"")){
									startMultiValue = 0;
									multiValue = multiValue + ","+ element;
									valueMap.put(elementIndeX.get(valueIndex), multiValue.substring(1,multiValue.length()-1));
									multiValue = "";
									valueIndex++;
									continue;
								}
							multiValue = multiValue + ","+ element;
							continue;
							}
							else if(element.startsWith("\"")){
								startMultiValue = 1;
								multiValue = multiValue + element;
								continue;
							}
							else{
								valueMap.put(elementIndeX.get(valueIndex), element);
								valueIndex++;	
							}
						}
					} 
					headerIndex++;
					dictCSV.put(key,valueMap);

				}
				
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
	}


	/**
	 * @param headerIndex
	 * 			Name of the CDF_header
	 * @return HashMap
	 * 			Hash-map with key:Header and Value: Value of header from the metafile1.
	 */
	public HashMap<String, String> getRowForGivenHeaderIndex(String headerIndex){
		HashMap<String,String> particularRow = dictCSV.get(headerIndex);
		return particularRow;
	}

	/**
	 * @param headerIndex
	 * 			Name of the CDF_header
	 * @param columnName
	 * 			Name of Column of metafile1
	 * @return String
	 * 			Value of corresponding Column from metafile1.
	 */
	public String getCellValueForParticularRow(String headerIndex, String columnName){
		HashMap<String,String> particularRow = dictCSV.get(headerIndex);
                if(particularRow == null)
                    return null;
		String cellValue = particularRow.get(columnName);
		return cellValue;
	}


	/**
	 * @param columnName
	 * 			Name of Column of metafile1
	 * @return ArrayList
	 * 			List of Values for all cdf_header from metafile1.
	 */
	public ArrayList<String> getParticularColumnForAllRow(String columnName){
		ArrayList<String> columnList = new ArrayList<String>();
		Set set = dictCSV.entrySet();
		Iterator i = set.iterator();
		while(i.hasNext()){
			Map.Entry me = (Map.Entry)i.next();
			HashMap<String, String> row = (HashMap<String, String>)me.getValue();
			columnList.add(row.get(columnName));
		}
		return columnList;
	}
	
	/**
	 * 
	 * @return ArrayList
	 * 			List of all cdf_header from metafile1.
	 */
        public ArrayList<String> getIndexColumnForAllRow(){
		ArrayList<String> columnList = new ArrayList<String>();
		Set set = dictCSV.entrySet();
		Iterator i = set.iterator();
		while(i.hasNext()){
			Map.Entry me = (Map.Entry)i.next();
			columnList.add((String)me.getKey());
		}
		return columnList;
	}
}