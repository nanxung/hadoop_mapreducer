package com.cb.mr.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class mySort extends WritableComparator {
	public mySort() {
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

			int r2 = Integer.compare(k1.getMonth(), k2.getMonth());

			if (r2 == 0) {

				return -Double.compare(k1.getHot(), k2.getHot());

			} else {

				return r2;
			}

		} else {

			return r1;

		}
	}

}
