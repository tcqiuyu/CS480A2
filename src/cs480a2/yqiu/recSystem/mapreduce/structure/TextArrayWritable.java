package cs480a2.yqiu.recSystem.mapreduce.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created by Qiu on 3/18/15.
 * The generic int array writable class
 */

public abstract class TextArrayWritable extends ArrayWritable {

    private Writable[] values;

    public TextArrayWritable(String[] textArray) {
        super(IntWritable.class);
        values = new Writable[textArray.length];
        initValues(textArray);
    }

    public void initValues(String[] textArray) {
        for (int i = 0; i < textArray.length; i++) {
            Writable text = new Text(textArray[i]);
            values[i] = text;
        }
    }

    public Writable[] getValues() {
        return values;
    }
}
