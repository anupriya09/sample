package tcs.poc.surveillance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.bson.Document;

public class OptimizationRunnable implements Runnable {

	private String uid;

	public OptimizationRunnable(String uid) {
		// TODO Auto-generated constructor stub
		this.uid = uid;
	}

	@Override
	public void run() {

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("F:/iPrescribe/BuyingProbCategory1.csv"));
			
			HashMap<String, Integer> prodHashMap = API0_Optimization.userProdMap.get(uid);
			long totalCount = 0;

			Iterator<Entry<String, Integer>> itr = prodHashMap.entrySet().iterator();

			while (itr.hasNext()) {
				Entry<String, Integer> entry = itr.next();
				totalCount += entry.getValue();
			}

			Double buyingProb = 0.0;

			Iterator<Entry<String, Integer>> itrN = prodHashMap.entrySet().iterator();
			
			while (itrN.hasNext()) {
				Document buyingProbDoc = new Document();
				buyingProb = 0.0;
				Entry<String, Integer> entry = itrN.next();
				buyingProb = (double) (entry.getValue() / totalCount);
				
				buyingProbDoc.append("bprob", buyingProb);
				buyingProbDoc.append("count", totalCount);
				
				System.out.println(uid + "," + entry.getKey()+","+ buyingProb);
				API0_Optimization.buyingProbStreamer.addData(uid + "_" + entry.getKey(), buyingProbDoc.toJson());
				//bw.write(entry.getKey()+","+entry.getValue()+","+buyingProb);bw.write("\n");
			}
			
			bw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
}
