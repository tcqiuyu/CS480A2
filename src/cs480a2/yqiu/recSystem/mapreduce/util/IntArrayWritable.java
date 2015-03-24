package cs480a2.yqiu.recSystem.mapreduce.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by Qiu on 3/18/15.
 * The generic int array writable class
 */

public abstract class IntArrayWritable extends ArrayWritable {

    private Writable[] values;

    public IntArrayWritable(int[] intsArray) {
        super(IntWritable.class);
        values = new Writable[intsArray.length];
        initValues(intsArray);
    }

    public void initValues(int[] intsArray) {
        for (int i = 0; i < intsArray.length; i++) {
            Writable value = new IntWritable(intsArray[i]);
            values[i] = value;
        }
    }

    public Writable[] getValues() {
        return values;
    }
}
