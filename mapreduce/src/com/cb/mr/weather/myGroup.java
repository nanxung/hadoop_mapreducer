package com.cb.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class myGroup extends WritableComparator {
	public myGroup() {
		// TODO Auto-generated constructor stub
		super(myKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		myKey k1 = (myKey) a;
		myKey k2 = (myKey) b;
		int r1 = Integer.compare(k1.getYear(), k2.getYear());
		if (r1 == 0) {

			return Integer.compare(k1.getMonth(), k2.getMonth());
			
		} else {

			return r1;

		}
	}

}