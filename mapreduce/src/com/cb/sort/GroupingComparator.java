package com.cb.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if(Integer.parseInt(a.toString().split(" ")[0])==Integer.parseInt(b.toString().split(" ")[0])){
			return 0;
		}else if(Integer.parseInt(a.toString().split(" ")[0])>Integer.parseInt(b.toString().split(" ")[0])){
				return 1;
			}else if(Integer.parseInt(a.toString().split(" ")[0])<Integer.parseInt(b.toString().split(" ")[0])){
				return -1;
			
		}
		return 0;
	}
	

}
