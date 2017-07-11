import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class H1B3 {
	
public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		
		String[] line = value.toString().split("\t");
		String soc_name=line[3].toUpperCase();
		String job_title = line[4].toUpperCase();
		//int count=0;
		if(job_title.contains("DATA SCIENTIST"))
		{
			
		context.write(new Text(soc_name), new IntWritable(1));
		}
	}
}
public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	int max=0;
Text socname= new Text();
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{int sum=0;
		for(IntWritable t: value)
		{
			sum+=t.get();
		}
		if(sum>max)
		{
			max=sum;
			socname.set(key);
		}
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		//String out = "the name of the Employer who has most no of petition is "+key;
		context.write(new Text(socname), new IntWritable(max));
	}
}
public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"count");
	job.setJarByClass(H1B3.class);
	job.setMapperClass(MyMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(MyReducer.class);
    FileInputFormat.addInputPath(job, new Path (args[0]));
    FileOutputFormat.setOutputPath(job, new Path (args[1]));
    System.exit(job.waitForCompletion(true)? 0:1);
}
}
