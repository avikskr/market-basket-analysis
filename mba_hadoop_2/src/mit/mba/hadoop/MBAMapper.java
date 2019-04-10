package mit.mba.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/*For each <itemset,support> pair say <{a,b,c} 1> Mapper will emit
<{a,b,c} 1>
<{b,c} {a,b,c;1}>
<{a,c} {a,b,c;1}>
<{a,b} {a,b,c;1}>
<{a} {a,b,c;1}>
<{b} {a,b,c;1}>
<{c} {a,b,c;1}>
Ignore {} braces, it has just been added here to highlight key and value separately.
*/
public class MBAMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] pair = value.toString().split("\t");
		String itemset = pair[0].replaceAll("[\\p{Ps}\\p{Pe}]", "").trim();//removes braces
		String supportCount = pair[1].trim();
		StringTokenizer itm = new StringTokenizer(itemset, ",");
		String[] items = new String[itm.countTokens()];
		int i = 0;
		while(itm.hasMoreTokens())
			items[i++] = itm.nextToken();
		outkey.set(itemset);
		outvalue.set(supportCount);
		context.write(outkey, outvalue);
		
		if(items.length > 1 ) {
			List<String> com=Combination.getItemSets(items);
			for(String v:com){
				outkey.set(v.toString().replaceAll("[\\p{Ps}\\p{Pe}]", ""));
				outvalue.set(itemset+";"+supportCount);
				context.write(outkey, outvalue);
			}
		}
	
	}
}