package mit.mba.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/* for perticular key mapper will emit the sum of the value
  for example: 
  key : [a,b]
  value : 59
  output: [a,b] 59
 */
public class MBAReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum=0;
		for(IntWritable v:values){
			sum=sum+v.get();
		}
		//float support=sum/10;
		//outvalue.set(sum);
		
		context.write(key,new IntWritable(sum));
	
		}
	
}
