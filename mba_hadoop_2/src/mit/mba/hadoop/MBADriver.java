package mit.mba.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]+"/part-r-00000"));
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

/* final output:
[apples] => [heineken]	{0.104895 , 0.334395}
[apples] => [avocado]	{0.138861 , 0.442675}
[apples] => [peppers,sardines]	{0.090909 , 0.289809}
[apples] => [steak]	{0.117882 , 0.375796}
[apples] => [corned_b]	{0.150849 , 0.480892}
[apples] => [steak,hering,corned_b,olives]	{0.096903 , 0.308917}
[apples] => [baguette]	{0.146853 , 0.468153}
[apples] => [avocado,sardines,baguette]	{0.091908 , 0.292994}
[apples] => [peppers,avocado,baguette]	{0.091908 , 0.292994}
[apples] => [steak,corned_b]	{0.101898 , 0.324841}
[apples] => [steak,olives]	{0.098901 , 0.315287}
[apples] => [steak,hering,corned_b]	{0.096903 , 0.308917} 
....
 */