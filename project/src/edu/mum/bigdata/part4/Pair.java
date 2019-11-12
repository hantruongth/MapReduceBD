package edu.mum.bigdata.part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair>{
		private Text first;
		private Text second;
		private String delim = ",";
		
		public Pair() {
			set(new Text(), new Text());
		}
		
		public Pair(String first, String second) {
			set(new Text(first), new Text(second));
		}
		
		public void set(Text first, Text second) {
			this.first = first;
			this.second = second;
		}
		
		public Text getFirst() {
			return first;
		}
		
		public String getFirstString() {
			return new String(first.toString());
		}
		
		public String getSecondString() {
			return new String(second.toString());
		}
		public Text getSecond() {
			return second;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		}
		
		@Override
		public int hashCode() {
			return first.hashCode() * 163 + second.hashCode();
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof Pair) {
				Pair tp = (Pair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}
		
		@Override
		public int compareTo(Pair tp) {
			int cmp = first.compareTo(tp.first);
			if (cmp != 0) {
				return cmp;
			}
			return second.compareTo(tp.second);
		}
		public void setDelim(String delim) {
			this.delim = delim;
		}

		public String toString() {
			return "" + first + delim + second;
		}
	}