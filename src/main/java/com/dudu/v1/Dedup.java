package com.dudu.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Think
 * Date: 13-12-5
 * Time: 下午12:33
 * To change this template use File | Settings | File Templates.
 */
public class Dedup {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Dedup <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Date Deduplication");
        job.setJarByClass(Dedup.class);
        job.setMapperClass(MyMap.class);
//        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    //map 将输入中的value 复制到输出数据的key上,并执行输出
    public static class MyMap extends Mapper<Object, Text, Text, Text> {
        private static String line = new String();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value.toString();
            if (line.charAt(0) == '1' || line.charAt(0) == '2') {
                line = line.substring(1);
                String[] split = line.split(",");
                context.write(new Text(split[0]), value);
            }
        }
    }

    //reduce 将输入中的key复制到输出数据的key上,并直接输出
    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            List<String> list = new ArrayList<String>();
            for (; it.hasNext(); ) {
                list.add(it.next().toString());
            }
            if (list.size() != 2 && list.size() > 0) {
                for (String t : list) {
                    context.write(new Text(t), new Text(""));
                }
            }
        }
    }
}
