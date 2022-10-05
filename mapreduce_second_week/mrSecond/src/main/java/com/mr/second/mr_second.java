package com.mr.second;
import java.io.IOException;
import java.util.StringTokenizer;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class mr_second {

    //Mapper函数
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int maxValue = 0;

        //这里面的one、word都是类似于String一样的类，可以使用set设置其值。
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Now in Mapper.");
            //value是传递过来的数据块；StringTokenizer是字符串分词器通过delim参数将数据分成一部分一部分，返回类似迭代器。
            //最终通过map将数据存储在context中。
            StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
            System.out.println("Mapper:"+value.toString());
            while (itr.hasMoreTokens()) {
                String[] str = itr.nextToken().split(" ");
                String name = str[0];
                one.set(Integer.parseInt(str[1]));
                word.set(name);
                context.write(word,one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        //此时传到reduce的数据是经过排序之后的相同的key以及下面的value。下面代码是保留相同key中最大的值。
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("Reduce:");
            int sumNum = 0;
            for (IntWritable intWritable : values) {
                sumNum  += intWritable.get();
            }
            result.set(sumNum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("sssssssss");
        Configuration conf = new Configuration();
        Job job = new Job(conf, "mr_second");
        job.setJarByClass(mr_second.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        String input_path = "/Usd/src/main/java/com/mr/second/input_file.txt";
        String input_path = "/Users/zhanghuaibei/Documents/java_scala_spark_flink/spark_practice/geek_BigData_Train/mapreduce_second_week/mrSecond/src/main/java/com/mr/second/input.txt";
//        String input_path = args[0];
//        String output_path = args[1];
//        String output_path = "/Usersn/java/com/mr/second/output_file";
        String output_path = "/Users/zhanghuaibei/Documents/java_scala_spark_flink/spark_practice/geek_BigData_Train/mapreduce_second_week/mrSecond/src/main/java/com/mr/second/output";
//        FileInputFormat.addInputPath(job, new Path(input_path));
        FileInputFormat.addInputPath(job, new Path(input_path));
//        FileOutputFormat.setOutputPath(job, new Path(output_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.waitForCompletion(true);

    }


}
