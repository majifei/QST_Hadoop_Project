package bb.bb;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 跑IosAndroid.java输出的文件
 * 
 * @author leo
 *
 */
public class PhoneCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "countPhone");
		job.setMapperClass(concurrentMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/testdir/file22mResult"));

		Boolean status = job.waitForCompletion(true);
		if (status) {
			Counters counters = job.getCounters();
			Counter counter1 = counters.findCounter("count", "Android");
			Counter counter2 = counters.findCounter("count", "Ios");
			System.out.println("Android----->" + counter1.getValue() + "----------");
			System.out.println("Ios----------->" + counter2.getValue() + "----------");
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/testdir/file22mResult"), conf);
			fs.delete(new Path("hdfs://localhost:9000/testdir/file22mResult"), true);// 删除空的输出路径
		}

	}

	public static class concurrentMap extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) {

			String phone = value.toString().split(" ")[1];
			Counter ct1 = context.getCounter("count", "Android");
			Counter ct2 = context.getCounter("count", "Ios");
			// equals!!
			if (phone.equals("Android")) {

				ct1.increment(1);

			} else {

				ct2.increment(1);
			}

		}
	}

}
