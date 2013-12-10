package com.dudu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: Think
 * Date: 13-12-5
 * Time: 下午12:33
 * To change this template use File | Settings | File Templates.
 */
public class MyTest2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MyTest <in> <out>");
            System.exit(2);
        }
        //job1
        Job job1 = new Job(conf, "MyTest1");
        job1.setJarByClass(MyTest.class);
        job1.setMapperClass(MyTest.MyMap.class);
        job1.setCombinerClass(MyTest.MyReduce.class);
        job1.setReducerClass(MyTest.MyReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        //job2
        Job job2 = new Job(conf, "MyTest2");
        job2.setJarByClass(MyTest2.class);
        job2.setMapperClass(MyMap2.class);
//        job2.setCombinerClass(MyReduce2.class);
//        job2.setReducerClass(MyReduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_2"));

        ControlledJob controlledJob1 = new ControlledJob(job1, null);
        ControlledJob controlledJob2 = new ControlledJob(job2, null);
        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("test job group");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.run();
        System.out.println("finished ");
        System.exit(jobControl.allFinished() ? 0 : 1);
        System.out.println(". ");
    }

    //map 将输入中的value 复制到输出数据的key上,并执行输出
    public static class MyMap2 extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();
        private static Text _key = new Text();
        private static Text _val = new Text();
        private static List<String> _list = new ArrayList<String>();
        private static StringBuffer outKeyBuffer = new StringBuffer();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            _list.clear();
            StringTokenizer itr = new StringTokenizer(line.toString());
            int a = 0;
            while (itr.hasMoreTokens()) {
                if (a == 0) {
                    _key.set(itr.nextToken());
                } else if (a == 1) {
                    _val.set(itr.nextToken()
//                            .replaceAll("#*","#")
                    );
                }
                a++;
            }
            String[] split = _val.toString().split("#");
            for (String s : split) {
                if (s != null && s.trim().length() > 10) {
                    _list.add(s);
                }
            }
            if (_list.size() > 3) {
                java.util.Collections.sort(_list, new Comparator<String>() {
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
                            System.out.println("转换异常.[" + o1 + "],金额:" + splitOne1[2]);
                            System.out.println("or 转换异常.[" + o2 + "],金额:" + splitOne2[2]);
                        }
                        return splitOne1[3].compareTo(splitOne2[3]) == 0
                                ? amount1 > amount2 ? -1 : 1
                                : splitOne1[3].compareTo(splitOne2[3]);
                    }
                });
                int i = 1;
                for (String tmp : _list) {
                    outKeyBuffer.delete(0, outKeyBuffer.length());
                    String[] splitOne = tmp.split(",");
                    outKeyBuffer.append(splitOne[1]).append(",").append(splitOne[0]).append(",").append(i++)
                    //.append("\t").append(tmp)
                    ;
                    context.write(new Text(outKeyBuffer.toString()), new Text(""));
                }
            }
        }
    }

//    //reduce 将输入中的key复制到输出数据的key上,并直接输出
//    public static class MyReduce2 extends Reducer<Text, Text, Text, Text> {
//        private static StringBuffer buffer = new StringBuffer();
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            buffer.delete(0, buffer.length());
//            for (Iterator<Text> iterator1 = values.iterator(); iterator1.hasNext(); ) {
//                buffer.append(iterator1.next()).append("#");
//            }
//            context.write(key, new Text(buffer.toString()));
//        }
//    }
}
