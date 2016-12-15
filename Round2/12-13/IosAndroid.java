package bb.bb;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 全跑ip去重，输出格式：
 * 
 * 2016-01-02 Ios 101.226.103.59
 * 
 * 再进行PV
 * 
 * 
 * @author leo
 *
 */
public class IosAndroid {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "phones");
		job.setJarByClass(IosAndroid.class);

		job.setMapperClass(phoneMapper.class);
		job.setCombinerClass(filterReduce.class);
		job.setReducerClass(filterReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

	public static class phoneMapper extends Mapper<Object, Text, Text, Text> {

		Text ipphone = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String string = value.toString();
			String ip = string.split(" ")[0];

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathName = fileSplit.getPath().getParent().getName();

			String patternAndroid = "Android*";
			String patternIos = "iPhone*";
			Pattern r1 = Pattern.compile(patternAndroid);
			Pattern r2 = Pattern.compile(patternIos);
			Matcher m1 = r1.matcher(string);
			Matcher m2 = r2.matcher(string);

			if (m1.find()) {
				ipphone.set(pathName + " Android " + ip);
				context.write(ipphone, new Text(""));
			}
			if (m2.find()) {
				ipphone.set(pathName + " Ios " + ip);
				context.write(ipphone, new Text(""));
			}
		}
	}

	public static class filterReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			context.write(key, new Text(""));

		}

	}
}
