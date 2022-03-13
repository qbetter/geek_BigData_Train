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

最终的展示结果：




main函数这部分因为不懂这些设置的意思所以一个bug调试了很久。原因主要是：



