package mit.mba.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MBADriver {

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(MBADriver.class);
		job.setMapperClass(MBAMapper.class);
		job.setReducerClass(MBAReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

/*   final  output: 
[apples,baguette]	147
[apples,olives,baguette]	35
[apples,olives]	159
[apples,sardines,baguette]	95
[apples,turkey,soda,sardines]	1
[apples,turkey,soda]	4
[apples,turkey]	58
[apples]	314
[artichok,apples,baguette]	33
[artichok,apples,olives,baguette]	2
[artichok,apples,olives]	21
[artichok,apples,sardines,baguette]	11
...
 */
