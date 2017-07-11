import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1B9 {
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String [] line=value.toString().split("\t");
		String employers=line[2].toLowerCase();
		String status =line[1].toLowerCase();
		
		context.write(new Text(employers), new Text (status));
		
	}
}
public static class MyReducer extends Reducer<Text,Text,Text,FloatWritable>
{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		float perc=0;
		float count =0, count1=0; 
		for(Text it:value)
		{
			
			count++;
			
			if(count>1000)
			{
			if(it.toString().contains("certified"))
			{
				count1++;
			}
			}
		}
		
		perc=(count1*100)/count;
		if(perc>70.0)
		{
		String out	=key+" "+ count +" "+count1;
		context.write(new Text(out),new FloatWritable(perc));
		}
	}
}
public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"movie count");
	job.setJarByClass(H1B9.class);
	job.setMapperClass(MyMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(MyReducer.class);
    FileInputFormat.addInputPath(job, new Path (args[0]));
    FileOutputFormat.setOutputPath(job, new Path (args[1]));
    System.exit(job.waitForCompletion(true)? 0:1);
}
}
