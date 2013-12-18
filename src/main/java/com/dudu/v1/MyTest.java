package com.dudu.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: Think
 * Date: 13-12-5
 * Time: 下午12:33
 * To change this template use File | Settings | File Templates.
 */
public class MyTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MyTest <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MyTest");
        job.setJarByClass(MyTest.class);
        job.setMapperClass(MyMap.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        JobControl jobControl = new JobControl("test job group");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //map 将输入中的value 复制到输出数据的key上,并执行输出
    public static class MyMap extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();
        private static String _key = null;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            String[] split = line.toString().split(",");
            _key = split[0]
//                    +","+split[3]
            ;
            double amount = Double.parseDouble(split[2]);
            if (amount>=10) {
                context.write(new Text(_key), line);
            }
        }
    }

    //reduce 将输入中的key复制到输出数据的key上,并直接输出
    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        private static StringBuffer buffer = new StringBuffer();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            buffer.delete(0, buffer.length());
            for (Iterator<Text> iterator1 = values.iterator(); iterator1.hasNext(); ) {
                buffer.append(iterator1.next()).append("#");
            }
            context.write(key, new Text(buffer.toString()));
        }
    }
}
