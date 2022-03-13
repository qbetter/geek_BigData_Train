package com.mr.second.Bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


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
//
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

    @Override
    public String toString() {
        return up_flow+"\t"+down_flow+"\t"+sum_flow;
    }

    public static void main(String[] args) {

        FlowBean flowBean = new FlowBean(300, 450);

        System.out.println("up flow is:"+flowBean.up_flow+"\t down flow is:"+ flowBean.down_flow);

    }
}
