package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This is combiner to calculate TF
 * Input key: Text --- > Title
 * Input value: TextArrayWritable ---> [ word, "1", "1" ]
 * Output key: Text ---> Word
 * Output value: TextArrayWritable ---> [ title, freq. of the word, total word count for the book ]
 */

public class CustomCombiner extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    @Override
    protected void reduce(Text title, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

    }
}
