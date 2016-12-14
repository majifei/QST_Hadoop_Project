package bb.bb;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * 百度跳转PV
 * 
 * [a-zA-z]+://www.baidu.*
 * 
 * @author leo
 *
 */

public class baiduPV {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String string = value.toString();
			String pattern = "[a-zA-z]+://www.baidu.*";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(string);

			Counter ct = context.getCounter("c", "sum");
			if (m.find()) {

				ct.increment(1);

			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "keepDay");
		job.setJarByClass(baiduPV.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/testdir/file22mResult"));

		Boolean status = job.waitForCompletion(true);
		if (status) {
			Counters counters = job.getCounters();
			Counter counter1 = counters.findCounter("c", "sum");
			System.out.println("----------" + counter1.getValue() + "----------");
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/testdir/file22mResult"), conf);
			fs.delete(new Path("hdfs://localhost:9000/testdir/file22mResult"), true);// 删除空的输出路径
		}

	}

}
