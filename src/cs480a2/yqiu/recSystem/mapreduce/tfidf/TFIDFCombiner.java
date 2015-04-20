package cs480a2.yqiu.recSystem.mapreduce.tfidf;

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

//public class TFIDFCombiner extends Reducer<Text, Text, Text, Text> {
    public class TFIDFCombiner extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    @Override
    protected void reduce(Text title, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        CustomMap tempMap = new CustomMap();
//        throw new IOException("Total book count: " + context.getConfiguration().getDouble("Total.Book.Count", 0));
        //compute word count and store them in a map
//        for (Text val : values) {
        for (TextArrayWritable val : values) {
            //get word
            Text word = (Text) val.get()[0];
//            Text word = val;
//            update the specific word count
            tempMap.increment(word, 1);
        }

        /*
         * Wordset of the book
         * Key: word
         * Value: word count within the book
         */
        Set<Entry<Text, Integer>> entries = tempMap.entrySet();

        Integer maxWordCount = Collections.max(tempMap.values());
        Text maxWordCountText = new Text(maxWordCount.toString());

        for (Entry<Text, Integer> entry : entries) {
            Text wordCount = new Text(entry.getValue().toString());
            //output value
            TextArrayWritable outVal = new TextArrayWritable(new Text[]{title, wordCount, maxWordCountText});
//            TextArrayWritable outVal = new TextArrayWritable(new Text[]{title, wordCount, maxWordCountText});
//            context.write(entry.getKey(), outVal);

            context.write(entry.getKey(), outVal);
//            throw new IOException("Key: " + entry.getKey() + " --- val: " + outVal);
        }
//
//        for (TextArrayWritable val : values) {
//            context.write((Text) val.get()[0], new TextArrayWritable(new Text[]{title}));
//        }
    }

}
