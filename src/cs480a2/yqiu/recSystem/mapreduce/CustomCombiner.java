package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by Qiu on 3/18/15.
 * This is combiner to calculate TF
 * Input key: Text --- > Title
 * Input value: TextArrayWritable ---> [ word, "1", "1" ]
 * Output key: Text ---> Word
 * Output value: TextArrayWritable ---> [ title, word count for the key, total word count for the book ]
 */

public class CustomCombiner extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    @Override
    protected void reduce(Text title, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

        CustomMap tempMap = new CustomMap();

        throw new IOException("Total book count: " + context.getConfiguration().getDouble("Total.Book.Count", 0));
//        //compute word count and store them in a map
//        for (TextArrayWritable val : values) {
//            //get word
//            Text word = (Text) val.get()[0];
//            //update the specific word count
//            tempMap.increment(word, 1);
//        }
//
//        /*
//         * Wordset of the book
//         * Key: word
//         * Value: word count within the book
//         */
//        Set<Entry<Text, Integer>> entries = tempMap.entrySet();
//
//        Integer maxWordCount = Collections.max(tempMap.values());
//        Text maxWordCountText = new Text(maxWordCount.toString());
//
//        for (Entry<Text, Integer> entry : entries) {
//            Text wordCount = new Text(entry.getValue().toString());
//            //output value
//            TextArrayWritable outVal = new TextArrayWritable(new Text[]{title, wordCount, maxWordCountText});
////            context.write(entry.getKey(), outVal);
//        }
    }

    private class CustomMap extends HashMap<Text, Integer> {

        public void increment(Text text, Integer val) {
            Integer prev = get(text);
            if (prev != null) {
                val += prev;
            }
            put(text, val);
        }
    }
}
