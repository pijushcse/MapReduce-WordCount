package home;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable red_value = new IntWritable(1);
	private Text red_key = new Text();

	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString();
		StringTokenizer st = new StringTokenizer(line);

		while (st.hasMoreTokens()) {
			red_key.set(st.nextToken().toLowerCase());
			if (Character.isAlphabetic(red_key.toString().charAt(0))) {
				context.write(red_key, red_value);
			}

		}
	}
}
