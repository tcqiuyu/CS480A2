package cs480a2.yqiu.recSystem.mapreduce.tfidf;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import cs480a2.yqiu.recSystem.mapreduce.util.BookCounter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.awt.print.Book;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by Qiu on 3/18/15.
 * This is the reducer that calculate TF-IDF
 * Input Key: Text ---> Word
 * Input Value: TextArrayWritable ---> [ Title, Word Count, Maximum Word Count, "1" ]
 * Output Key: TextArrayWritable ---> "book title: word"
 * Output Value: DoubleWritable ---> TF-IDF Value
 */

public class TFIDFReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    private double totalBookCount;

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        //if the key is "!", it means values contain book count
        if (key.equals(new Text("!"))) {
            for (TextArrayWritable bookCounts : values) {
                Text bookCountText = ((Text) bookCounts.get()[0]);
                double bookCount = Double.parseDouble(bookCountText.toString());
                totalBookCount += bookCount;
            }
        }

        String word = key.toString();

        // Need to iterate values twice: One for calculate book occurance, another for emit each word to file one by one.
        // Since iterator cannot be reset, I create one more hashset.
        HashSet<TextArrayWritable> valSet = new HashSet<TextArrayWritable>();
        for (TextArrayWritable val : values) {
            valSet.add(val);
        }

        double bookOccurCount = valSet.size();

        for (TextArrayWritable val : valSet) {

            Writable[] inputVal = val.get();
            String title = inputVal[0].toString();
            double wordCount = Double.parseDouble(inputVal[1].toString());
            double maxWordCount = Double.parseDouble(inputVal[2].toString());
            double tf = wordCount / maxWordCount;
            double idf = Math.log(totalBookCount / bookOccurCount) / Math.log(2);
            Double tfidf = tf*idf;

            Text outKey = new Text(title);
            Text[] outValText = new Text[]{new Text(word), new Text(tfidf.toString())};

            TextArrayWritable outVal = new TextArrayWritable(outValText);
            context.write(outKey, outVal);

        }
    }
}
