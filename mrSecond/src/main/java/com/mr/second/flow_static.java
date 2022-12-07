package com.mr.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;

import com.mr.second.Bean.FlowBean;



public class flow_static {

    //mapper阶段以手机号为key将上行和下行流量存储到Bean中
    public static class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer IterToken = new StringTokenizer(value.toString(), "\n");
            while (IterToken.hasMoreTokens()) {
                String originStr = IterToken.nextToken();
                String[] split = originStr.split("\t");
//                System.out.println(originStr);
                String phone_num = split[1];
                String upflow = split[9];
                String downflow = split[10];
                String flows = upflow + "\t" + downflow;
                FlowBean flowBean = new FlowBean(Long.parseLong(upflow), Long.parseLong(downflow));
//                FlowBean flowBean = new FlowBean();
                context.write(new Text(phone_num), flowBean);
            }


        }
    }

    public static class FlowReducer extends Reducer<Text,FlowBean,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//            System.out.println("reducereduce");
            long up_total = 0;
            long down_total = 0;
            long total = 0;
            for (FlowBean val:values){
                up_total += val.up_flow;
                down_total += val.down_flow;
                total = val.up_flow + val.down_flow;
            }
            String rest = up_total+"\t"+down_total+"\t"+total;
            context.write(key,new Text(rest));
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Flow_Static");
//        Job job = new Job(conf, "Flow_Static");

        job.setJarByClass(flow_static.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(1);

//        String input_path = args[0];
//        String output_path = args[1];
        String input_path = "/Users/zhanghuaibei/Documents/java_scala_spark_flink/spark_practice/geek_BigData_Train/mrSecond/src/main/java/com/mr/second/HTTP_20130313143750.dat";
        String output_path = "/Users/zhanghuaibei/Documents/java_scala_spark_flink/spark_practice/geek_BigData_Train/mrSecond/src/main/java/com/mr/second/OUTput_file";

//        String input_path = "/Users/huaibei/Documents/ml/geek大数据/第二周MapReduce/mrSecond/src/main/java/com/mr/second/HTTP_20130313143750.dat";
//        String output_path = "/Users/huaibei/Documents/ml/geek大数据/第二周MapReduce/mrSecond/src/main/java/com/mr/second/SecondOutput";
        Path outputP = new Path(output_path);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputP)){
            fileSystem.delete(outputP,true);
        }
        FileInputFormat.addInputPath(job,new Path(input_path));
        FileOutputFormat.setOutputPath(job,new Path(output_path));

        job.waitForCompletion(true);
    }



}
