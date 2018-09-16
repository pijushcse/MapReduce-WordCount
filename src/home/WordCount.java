package home;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

 
public class WordCount {
	
	public static void main (String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job1 = new Job(conf);
		job1.setJarByClass(WordCount.class);
		job1.setJobName("Word Count");
		
		FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:54310/input_data"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:54310/outputtttt"));
		
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.waitForCompletion(true);
		
	}
}