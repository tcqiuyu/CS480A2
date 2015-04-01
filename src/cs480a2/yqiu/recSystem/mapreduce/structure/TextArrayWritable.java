package cs480a2.yqiu.recSystem.mapreduce.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by Qiu on 3/18/15.
 * The text array writable class
 */

public class TextArrayWritable extends ArrayWritable{
    //
//    private Writable[] values;
//
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] textArray) {
        super(Text.class, textArray);
    }

    //
//    public TextArrayWritable(String[] strArray) {
//        super(Text.class);
//        values = new Writable[strArray.length];
//
//        //convert string to text
//        for (int i = 0; i < strArray.length; i++) {
//            Writable text = new Text(strArray[i]);
//            values[i] = text;
//        }
//
//        set(values);
//    }

}
