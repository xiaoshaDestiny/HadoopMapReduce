package com.learn.flowsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * �������������������������� 
 * ���л��ͷ����л�����
 * @author Administrator
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;   //��������
	private long downFlow; //��������
	private long sumFlow;  //������
	
	
	//�޲ι�����  Ϊ�˺����ķ���
	public FlowBean() {
		super();
	}

	//�вι����� ��ʼ��
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	//���
	public void set(long upFlow, long downFlow){
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	//���л�����   Map��Reduceд
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	
	//�����л�����  Reduce����Map������
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

	
	//������Ҫ��д�ķ��� ���ս�����
	@Override
	public int compareTo(FlowBean o) {
		return (int) (this.sumFlow - o.getSumFlow());
	}
}
