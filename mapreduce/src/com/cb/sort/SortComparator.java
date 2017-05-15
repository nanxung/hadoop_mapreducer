package com.cb.sort;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SortComparator extends WritableComparator {
	public SortComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		System.out.println("a:"+a.toString());
		System.out.println("b:"+b.toString());

		if(Integer.parseInt(a.toString().split(",")[0].trim())==Integer.parseInt(b.toString().split(",")[0].trim())){
			if(Integer.parseInt(a.toString().split(",")[1].trim())>Integer.parseInt(b.toString().split(",")[1].trim())){
				return 1;
			}else if(Integer.parseInt(a.toString().split(",")[1].trim())<Integer.parseInt(b.toString().split(",")[1].trim())) {
				return -1;
			}else if(Integer.parseInt(a.toString().split(",")[1].trim())==Integer.parseInt(b.toString().split(",")[1].trim())) {
				return 0;
			}
		}else{
			if(Integer.parseInt(a.toString().split(",")[0].trim())>Integer.parseInt(b.toString().split(",")[0].trim())){
				return 1;
			}else if(Integer.parseInt(a.toString().split(",")[0].trim())<Integer.parseInt(b.toString().split(",")[0].trim())){
				return -1;
			}
		}
		return 0;
	}

}
