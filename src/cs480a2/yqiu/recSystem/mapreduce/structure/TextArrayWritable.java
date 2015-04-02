package cs480a2.yqiu.recSystem.mapreduce.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created by Qiu on 3/18/15.
 * The text array writable class
 */

public class TextArrayWritable extends ArrayWritable {
    //
//    private Writable[] values;
//
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] textArray) {
        super(Text.class, textArray);
    }

    @Override
    public String toString() {
        Writable[] values = get();

        String string = "";
        for (Writable value : values) {
            Text valText = (Text) value;
            String tmp = valText.toString();
            string = string.concat(tmp + " ");
        }

        return string.substring(0, string.length() - 1);
    }

}
