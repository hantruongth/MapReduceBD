package edu.mum.bigdata.common;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable {

    public String toString() {
        String s = "[";
        int count = 0;
        for (Entry<Writable, Writable> item : entrySet()) {
            s += item.getKey() + " " + item.getValue();
            if (count < size() - 1)
                s += ",";

            count++;
        }
        return s + "]";
    }
}
