package com.dudu.v2;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * User: Think
 * Date: 13-12-17
 * Time: 下午7:07
 */
public class GongJiaoBad {
    private static Logger log = LoggerFactory.getLogger(GongJiaoBad.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: GongJiaoBad <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "GongJiaoBad");
        job.setJarByClass(GongJiaoBad.class);
        job.setMapperClass(GongJiaoMap.class);
//        job.setCombinerClass(GongJiaoReduce.class);
        job.setReducerClass(GongJiaoReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //map 将输入中的value 复制到输出数据的key上,并执行输出
    public static class GongJiaoMap extends Mapper<Object, Text, Text, Text> {
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
            if (amount >= 10 && line.toString().trim().length() > 10) {
                context.write(new Text(_key), line);
            }
        }
    }

    //reduce 将输入中的key复制到输出数据的key上,并直接输出
    public static class GongJiaoReduce extends Reducer<Text, Text, Text, Text> {
        private StringBuffer buffer = new StringBuffer();
        private List<String> list = new LinkedList<String>();
        private String[] splitOne;
        private String _tmpStr;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            buffer.delete(0, buffer.length());
            list.clear();
            splitOne = null;
            _tmpStr = null;

            for (Iterator<Text> iterator1 = values.iterator(); iterator1.hasNext(); ) {
                list.add(iterator1.next().toString());
            }
            if (list.size() > 1) {
                Iterator<String> iterator = list.iterator();
                // 终端号为kong的记录 终端号是个下划线 _ 过滤掉这些记录.
                for (; iterator.hasNext(); ) {
                    String next = iterator.next();
                    splitOne = next.split(",");
                    if (splitOne[3].length()<3) {
                        iterator.remove();
                    }
                }
                java.util.Collections.sort(list, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        String[] splitOne1 = o1.split(",");
                        String[] splitOne2 = o2.split(",");
                        double amount1 = 0;
                        double amount2 = 0;
                        try {
                            amount1 = Double.parseDouble(splitOne1[2]);
                            amount2 = Double.parseDouble(splitOne2[2]);
                        } catch (NumberFormatException e) {
                            log.error("转换异常.[" + o1 + "],金额:" + splitOne1[2]);
                            log.error("or 转换异常.[" + o2 + "],金额:" + splitOne2[2]);
                        }
                        return splitOne1[3].compareTo(splitOne2[3]) == 0
                                ? amount1 > amount2 ? -1 : 1
                                : splitOne1[3].compareTo(splitOne2[3]);
                    }
                });
                int i = 1;
                for (String tmp : list) {
                    splitOne = tmp.split(",");
                    if (_tmpStr == null) {
                        _tmpStr = splitOne[3];
                    } else if (!_tmpStr.equals(splitOne[3])) {
                        _tmpStr = splitOne[3];
                        i = 1;
                    }
                    buffer.append(splitOne[1]).append(",")
                            .append(splitOne[3]).append(",")
                            .append(splitOne[0]).append(",")
                            .append(i++);
                    context.write(new Text(buffer.toString()), new Text());
                    buffer.delete(0, buffer.length());
                }
            }
            splitOne = null;
            list.clear();
        }
    }
}
