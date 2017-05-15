package com.cb.mr.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class myKey implements WritableComparable<myKey> {
	private int year;
	private int month;
	private double hot;
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public double getHot() {
		return hot;
	}
	public void setHot(double hot) {
		this.hot = hot;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.year=arg0.readInt();
		this.month=arg0.readInt();
		this.hot=arg0.readDouble();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(year);
		arg0.writeInt(month);
		arg0.writeDouble(hot);
	}
	//判断是否是同一个对象，当该对象作为输出的key的时候
	@Override
	public int compareTo(myKey o) {
		// TODO Auto-generated method stub
		int r1=Integer.compare(this.year, o.getYear());
		if(r1==0){
			int r2=Integer.compare(this.month, o.getMonth());
			if(r2==0){
			double r3=	Double.compare(this.hot, o.getHot());
					if(r3==0){
					return 0;
				}
			}
		}
		return 1;
	}
	
}
