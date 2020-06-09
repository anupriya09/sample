/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Table;
import com.google.common.collect.HashBasedTable;

/**
 * This class processes the metafile2 and load it into the hashmap for easy indexing
 * of column values. 
 */
public class PreProcessMetafile2v2 {
	/**
	 *  Level1 = First level of dictionary hierarchy.
	 */
	String Level1; 
	/**
	 *  Level2 = Temporal or Non-temporal features.
	 */
	String Level2;
	/**
	 *  OuterKey = Children of 2nd level.
	 */
	String OuterKey;
	/**
	 *  InnerKey = Necessary funcitons to be applied on the corresponding Child_Entity_Features.
	 */
	String InnerKey;
	HashMap<String, String> value;
	Map<String, Table<String, String, HashMap<String, String>>> entityMap = new HashMap<String,Table<String, String, HashMap<String, String>>>();

	/**
	 * This class processes the metafile2 and load it into the hashmap for easy indexing
	 * of column values. 
	 * @param filename
	 * 			Metafile2
	 */
	PreProcessMetafile2v2(String filename) {
		
		try {
			Reader reader = new FileReader(filename);
			List<String> Header = CSV.parseLine(reader);
			List<String> list;

			while ((list = CSV.parseLine(reader)) != null) {
				value = new HashMap<>();
				for (int i = 0; i < Header.size(); i++) { // System.out.println(Header.get(i)+list.get(i));
					switch (Header.get(i)) {
					case "Level1":
						Level1 = list.get(i);
						break;
					case "Level2":
						Level2 = list.get(i);
						break;
					case "OuterKey":
						OuterKey = list.get(i);
						break;
					case "InnerKey":
						InnerKey = list.get(i);
						break;
					default:
						value.put(Header.get(i), list.get(i));
						break;
					}
				}

				
//*********************************** FOR GENERALIZATION ************************************************************************ 				
				if(entityMap.containsKey(Level1.concat(Level2)))
				{
					entityMap.get(Level1.concat(Level2)).put(OuterKey, InnerKey, value);
				}
				else
				{
					Table<String, String, HashMap<String, String>> entityTable = HashBasedTable.create();
					entityTable.put(OuterKey, InnerKey, value);
					entityMap.put(Level1.concat(Level2), entityTable);
				}
//*******************************************************************************************************************************				
			}
		} catch (Exception e) {
			System.out.println(e);
			System.out.println(e.getStackTrace()[0].getLineNumber());
		}

		System.out.println("EntityMap Size="+entityMap.size());
		System.out.println("Map KeySet"+entityMap.keySet().toString());
	}
}
