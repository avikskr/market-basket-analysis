package mit.mba.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/* for input {a,b,c} mapper will emit
   [a] 1
   [b] 1
   [c] 1
   [a,b] 1
   [b,c] 1
   [a,c] 1
   [a,b,c] 1
   
 */

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outkey = new Text();
	private IntWritable outvalue = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String tranList[] = value.toString().split(" ");
		Arrays.sort(tranList);
		String[] t=new HashSet<String>(Arrays.asList(tranList)).toArray(new String[0]);
											//toArray(new String[0]) returns a araay of string
		List<String> com=Combination.getItemSets(t);
		for(String v:com){
			outkey.set(v.toString());
			context.write(outkey, outvalue);
		}
	}

}