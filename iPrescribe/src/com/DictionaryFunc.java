/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com;

import org.bson.Document;
import java.util.List;
import static com.XGBLSTMLoaderMain.windows;

/**
 * This file is used to perform some dictionary functions like extracting a
 * inner-key document, change the inner-key value etc. Following are the defined
 * functions.
 */
public class DictionaryFunc {

	public DictionaryFunc() {
	}

	/**
	 * Replace the existing value of given key with new value.
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param givenValue
	 *            Given value to be replaced.
	 * @param newValue
	 *            New value to enter.
	 * @return Document 
	 * 				Return updated part of main dictionary.
	 */
	public static Document UpdateWithValue(Document globalDict, List<String> keyList, String givenValue,
			String newValue) {
		Document temp = new Document();
		int i, j;
		for (i = keyList.size(); i > 0; i--) {
			Document temp2 = globalDict;
			for (j = 0; j < i; j++) {
				temp2 = (Document) temp2.get(keyList.get(j));
			}
			if (temp.isEmpty()) {
				temp2.replace(givenValue, newValue);
				temp = (Document) temp2;
			} else {
				temp2.replace(givenValue, temp);
				temp = (Document) temp2;
			}
			givenValue = keyList.get(j - 1);
		}
		return temp;

	}

	/**
	 * Add new key to an existing inner document and put value as given new
	 * document.
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param keyName
	 *            Key at which new value is to be appended.
	 * @param newValue
	 *            New value to enter.
	 * @return Document 
	 * 			Return updated part of main dictionary.
	 */
	public static Document AppendWithDictionary(Document globalDict, List<String> keyList, String keyName,
			Document newValue) { // System.out.println("append with dictionary");
		Document temp = new Document();
		int i, j;
		for (i = keyList.size(); i > 0; i--) {
			Document temp2 = globalDict;
			for (j = 0; j < i; j++) {
				temp2 = (Document) temp2.get(keyList.get(j));
			}
			if (temp.isEmpty()) {
				temp2.append(keyName, newValue);
				temp = (Document) temp2;
			} else {
				temp2.replace(keyName, temp);
				temp = (Document) temp2;
			}
			keyName = keyList.get(j - 1);
		}
		// System.out.println("appendwithdocument"+temp.keySet());
		return temp;
	}

	/**
	 * Add new key to an existing inner document and put value as given newValue.
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param keyName
	 *            Key at which new key is to be appended.
	 * @param keyValue
	 *            New key to enter.
	 * @return Document 
	 * 			Return updated part of main dictionary.
	 */
	public static Document AppendWithValue(Document globalDict, List<String> keyList, String keyName, String keyValue) { 
		Document temp = new Document();
		int i, j;
		for (i = keyList.size(); i > 0; i--) {
			Document temp2 = globalDict;
			for (j = 0; j < i; j++) {
				temp2 = (Document) temp2.get((String) keyList.get(j));
				// System.out.println("Appendvalue"+temp2.keySet());
			}
			if (temp.isEmpty()) {
				temp = (Document) temp2.append(keyName, keyValue);
			} else {
				temp2.replace(keyName, temp);
				temp = (Document) temp2;
			}
			keyName = keyList.get(j - 1);
		}
		return temp;
	}

	/**
	 * Increment the value of existing key by 1.
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param keyName
	 *            Key at which value is to be incremented.
	 * @return Document 
	 * 				Return updated part of main dictionary.
	 */
	public static Document IncrementKeyValue(Document globalDict, List<String> keyList, String keyName) { // System.out.println("increment
																											// key
																											// value");
		Document temp = new Document();
		int i, j;
		for (i = keyList.size(); i > 0; i--) {
			Document temp2 = globalDict;
			for (j = 0; j < i; j++) {
				temp2 = (Document) temp2.get((String) keyList.get(j));
				// System.out.println("increment"+temp2.keySet());
			}
			if (temp.isEmpty()) { // System.out.println(temp2.get(keyName).getClass());
				int value = Integer.parseInt((temp2.get(keyName).toString()));

				temp2.replace(keyName, Integer.toString(value + 1));
				temp = (Document) temp2;
			} else {
				temp2.replace(keyName, temp);
				temp = (Document) temp2;
			}
			keyName = keyList.get(j - 1);
		}
		return temp;
	}

	/**
	 * Return the value of given existing inner key. 
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param keyName
	 *            Key whose value to be fetched.
	 * @return String 
	 * 				Return value of key.
	 */
	public static String getKeyValue(Document globalDict, List<String> keyList, String keyName) {
		Document temp = globalDict;
		int i;
		for (i = 0; i < keyList.size(); i++) {
			temp = (Document) temp.get((String) keyList.get(i));
			// System.out.println(temp.keySet());
		}

		return ((String) temp.get(keyName));
	}

	/**
	 * Return the existing inner document.  
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyList
	 *            List of included keys within dictionary
	 * @param keyName
	 *            Key whose value to be fetched.
	 * @return Document 
	 * 				Return value of key.
	 */
	public static Document getDocument(Document globalDict, List<String> keyList, String keyName) {
		Document temp = globalDict;
		// System.out.println("getdocument");
		int i;
		for (i = 0; i < keyList.size(); i++) {
			temp = (Document) temp.get((String) keyList.get(i)); // going into dictionary
			// System.out.println(temp.keySet());
		}

		return ((Document) temp.get(keyName));
	}

	/**
	 * Return the existing inner document.  
	 * 
	 * @param globalDict
	 *            Feature dictionary
	 * @param keyName
	 *            Key whose value to be fetched.
	 * @return Document 
	 * 				Return value of key.
	 */
	public static Document getDocument(Document globalDict, String keyName) {
		return ((Document) globalDict.get(keyName));
	}

	/**
	 * Generate an empty document with Non-temporal empty part.  
	 *
	 * @return Document 
	 * 				
	 */
	public static Document Dictionary() {
		Document doc = new Document();
		// doc.append("UserEventBased", new Document());
		// doc.append("ProductBased", new Document());
		return (new Document().append("All", doc));
	}

	/**
	 * Create a date-dictionary inside the globaldictionary.  
	 *
	 * @return Document 
	 * 				
	 */
	public static Document dateDictionary() {
		Document doc = new Document();
		// doc.append("UserEventBased", new Document());
		// doc.append("ProductBased", new Document());
		return doc;
	}

	/**
	 * Create the window-dictionary for an event or product. 
	 *
	 * @return Document 
	 * 				
	 */
	public static Document windowDictionary() {
		Document doc = new Document();
		String zero = Integer.toString(0);
		for (int addWindow : windows)
			doc.append("Count_Window" + Integer.toString(addWindow) + "days", zero);
		return doc;
	}
}
