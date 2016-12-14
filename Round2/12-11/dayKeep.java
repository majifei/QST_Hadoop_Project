package QST_Hadoop_Project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 次日留存，输入两日文件 ；
 * 
 * 结果在输出问件中
 * 
 * @author leo
 *
 */

public class dayKeep {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		Text path = new Text();
		Text ip = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] string = value.toString().split(" ");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathName = fileSplit.getPath().getParent().getName();
			path.set(pathName);
			ip.set(string[0]);
			context.write(ip, path);
		}

	}

	public static class IntSumReducer extends Reducer<Text, Text, LongWritable, Text> {
		Text comp = new Text();
		Text value = new Text();
		static long sum = 0;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			comp.set(values.iterator().next());

			while (values.iterator().hasNext()) {
				value = values.iterator().next();
				if (!comp.equals(value)) {
					sum++;

					break;
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			context.write(new LongWritable(sum), null);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "keepDay");
		job.setJarByClass(dayKeep.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}

}
