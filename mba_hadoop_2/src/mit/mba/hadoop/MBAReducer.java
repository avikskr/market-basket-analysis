package mit.mba.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/*Input for Reducer will be formatted as below, for example,
key -> a,b
values -> [ {a,b,c;4}, {a,b,d;3}, 6, {a,b,f;4} ]
where each value is say, a,b,c;4
and support of a,b is 6 here.
Ignore the {} braces it's only for illustration purpose.*/

public class MBAReducer extends Reducer<Text,Text,Text,Text>{

	 private Text ruleKey = new Text();
	    private Text ruleValue = new Text();
	    private HashSet<String> rules = new HashSet<String>();
	    private double minConfidence=0.25;
	    private int txnCount=1001;
	    
	    
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        HashMap<String, Integer> itemsetMap = new HashMap<String, Integer>();
	        double lhsSupport = 1.0;
	        
	        StringTokenizer l = new StringTokenizer(key.toString(), ",");
	        ArrayList<String> lhsItems = new ArrayList<String>();
	        while(l.hasMoreTokens())
	            lhsItems.add(l.nextToken());

	       
	        for(Text textItemset : values) { 
	            StringTokenizer item = new StringTokenizer(textItemset.toString(), ";");
	            String[] itemset = new String[item.countTokens()];
	            int i = 0;
	            while(item.hasMoreTokens())
	                itemset[i++] = item.nextToken();
	           
	            if(itemset.length == 1) // Support of lhs              
	                lhsSupport = 1.0*Double.parseDouble(itemset[0]) / txnCount;
	            else {
	                
	                String itemsetKey = itemset[0]; // rhs            
	                int supportCount = Integer.parseInt(itemset[1]); // Support value of that combination
	                itemsetMap.put(itemsetKey, supportCount);
	            }
	        }

	       
	        double ruleSupport,confidence;
	        String lhs, rhs;
	        
	        for(String itemset : itemsetMap.keySet()) {
	            StringTokenizer items = new StringTokenizer(itemset, ",");
	            HashSet<String> consequentItems = new HashSet<String>();
	            while(items.hasMoreTokens())
	                consequentItems.add(items.nextToken());
	            
	            consequentItems.removeAll(lhsItems);
	            
	            ruleSupport = 1.0*itemsetMap.get(itemset) / txnCount;
	            confidence = ruleSupport / lhsSupport;

	            // filter rules by minConfidence
	            if(confidence >= minConfidence) {
	                lhs = "["+key.toString()+"]";
	                rhs = consequentItems.toString().replaceAll("(, )+", ",").trim();//removing spaces
	                String rule = lhs + " => " + rhs;
	                String revRule = rhs + " => " + lhs;
	               if(rules.contains(rule) || rules.contains(revRule)) // Avoid duplicate and reverse rules
	                    continue;
	                rules.add(rule);
	                ruleKey.set(rule);
	                ruleValue.set("{"+String.format("%.6f", ruleSupport)+" , "+String.format("%.6f", confidence)+"}");
	                context.write(ruleKey, ruleValue);
	            }
	        }
		}
	
}
