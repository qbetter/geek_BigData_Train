第二周的作业MapReduce


作业的主函数是flow_static。
需要定义一个类FlowBean来存储上下行流量数据：
public class FlowBean implements Writable {

    public long up_flow;
    public long down_flow;
    public long sum_flow;

    public FlowBean() {
    }

    public FlowBean(long up, long down) {
        up_flow = up;
        down_flow = down;
    }

    public FlowBean(long up, long down, long sum) {
        up_flow = up;
        down_flow = down;
        sum_flow = sum;
    }

    public long getUp_flow(){
        return up_flow;
    }

    public long getDown_flow(){
        return down_flow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up_flow);
        dataOutput.writeLong(down_flow);
        dataOutput.writeLong(sum_flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        up_flow = dataInput.readLong();
        down_flow = dataInput.readLong();
        sum_flow = dataInput.readLong();

    }
}
然后就是主函数的MapReduce了。

1，首先是mapper部分，该部分主要是以手机号为key将上行和下行流量存储到Bean中
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
    
然后reduce部分将相同手机号码的上下行流量和总流量存储起来。
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

接着就是关键的主函数的设置了：
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

        String input_path = args[0];
        String output_path = args[1];

//        String input_path = "/Users/econd/HTTP_20130313143750.dat";
//        String output_path = "/Users/econd/SecondOutput";
        Path outputP = new Path(output_path);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputP)){
            fileSystem.delete(outputP,true);
        }
        FileInputFormat.addInputPath(job,new Path(input_path));
        FileOutputFormat.setOutputPath(job,new Path(output_path));

        job.waitForCompletion(true);
    }

执行：
将项目打成jar包，并通过scp上传到开发机上。并且将input数据上传到hdfs上，共程序使用，最终输出结果也是在hdfs上。

然后通过Hadoop命令来执行程序，命令如下：
hadoop jar /home/stude/second_task/mrSecond-1.0-SNAPSHOT.jar com.mr.second.flow_static /user/stude/second/HTTP_20130313143750.dat /user/stuond/output
后面两个是在hdfs的输入数据地址，以及数据的输出地址。

scp传递数据的方式：
scp target/mrSecond-1.0-SNAPSHOT_old.jar  studen@55.52.33:/home/studebei/second_task

最终的展示结果：
![image](https://user-images.githubusercontent.com/21261099/158055834-2cfe22ac-305f-4cfe-83f0-7b60bce94fb3.png)

遇到的深坑：
main函数这部分因为不懂这些设置的意思所以一个bug调试了很久。原因主要是：
原来这两个是同时设置map和reduce的key、value的输出格式。
setOutputKeyClass和setOutputValueClass是同时设置K2,V2和K3,V3的类型，也就是说以下代码：
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
2)当K2, V2 和K3 , V3类型不一致时（大多数情况）：
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
setMapOutputKeyClass和setMapOutputValueClass会覆盖setOutputKeyClass和setOutputValueClass设置的效果，这样一来，setMapOutputKeyClass和setMapOutputValueClass设置的是map的输出类型，而setOutputKeyClass和setOutputValueClass设置的就只是reduce的输出类型。

上述设置完成后，还需要注意一个地方，那就是setCombinerClass方法必须要注释掉，否则仍然会报错，原因是一般情况下会把reduce方法设置成combiner的类，而数据流是
Mapper<K1, V1, K2, V2> --> Combiner<K2, V2, K3, V3> -->
Reducer<K3, V3, K4, V4>
K3,V3和K4,V4类型不同，所以仍会报错，因此需要将setCombinerClass方法注释掉
//job.setCombinerClass(Reduce.class);


