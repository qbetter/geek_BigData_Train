package com.mr.second;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.mapreduce.bean.FlowBean;
import java.io.IOException;
import java.io.StringWriter;
import java.util.StringTokenizer;

public class phone_flow_sum {

    //map阶段把手机号的上行和下行存储起来；
    public static class TokenizerMapper extends Mapper<LongWritable,Text,Text, Text>{
        private Text phone = new Text();
        private Text up_load_flow = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("This in Mapper.");

            StringTokenizer IterToken = new StringTokenizer(value.toString(), "\n");
            while (IterToken.hasMoreTokens()){
                String originStr = IterToken.nextToken();
                String[] split = originStr.split("\t");
                System.out.println(originStr);
                String phone_num = split[1];
                String upflow = split[9];
                String downflow = split[10];
                String flows = upflow+"\t"+downflow;
                phone.set(phone_num);
                up_load_flow.set(flows);
                context.write(phone,up_load_flow);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        private Text rst_value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reduce:"+key);
            String rst = "";
            int sum_up =0;
            int sum_down =0;
            int sum_total =0;
            for (Text val:values){
                String[] splits = val.toString().split("\t");
                System.out.println(val.toString());
//                String[] splits = val.split("\t");
                int up = Integer.valueOf(splits[0]);
                int down = Integer.valueOf(splits[1]);
                sum_up += up;
                sum_down += down;
                sum_total = up + down;
            }
            rst = sum_up +"\t"+sum_down+"\t"+sum_total;
            context.write(key,new Text(rst));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf,"phone_flow_sum");
         Job job = new Job(conf, "phone_flow_sum");
        job.setJarByClass(phone_flow_sum.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        String input_path = args[0];
        String input_path = "/Users/huaibei/Documents/ml/geek大数据/第二周MapReduce/mrSecond/src/main/java/com/mr/second/HTTP_20130313143750.dat";
        String output_path = "/Users/huaibei/Documents/ml/geek大数据/第二周MapReduce/mrSecond/src/main/java/com/mr/second/SecondOutput.txt";
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.waitForCompletion(true);

    }

}
