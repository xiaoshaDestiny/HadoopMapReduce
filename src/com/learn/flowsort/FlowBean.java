package com.learn.flowsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 上行流量、下行流量、总流量 
 * 序列化和反序列化方法
 * @author Administrator
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;   //上行流量
	private long downFlow; //下行流量
	private long sumFlow;  //总流量
	
	
	//无参构造器  为了后续的反射
	public FlowBean() {
		super();
	}

	//有参构造器 初始化
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	//求和
	public void set(long upFlow, long downFlow){
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	//序列化方法   Map王Reduce写
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	
	//反序列化方法  Reduce接收Map的数据
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();		
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	
	//排序需要重写的方法 按照降序列
	@Override
	public int compareTo(FlowBean o) {
		return (int) (this.sumFlow - o.getSumFlow());
	}
}
