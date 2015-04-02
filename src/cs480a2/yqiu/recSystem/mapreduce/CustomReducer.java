package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by Qiu on 3/18/15.
 * This is the reducer that calculate TF-IDF
 * Input key: Text ---> Word
 * Input value: TextArrayWritable ---> [ book title, word count, maximum word count for the book ]
 * Output key: TextArrayWritable ---> [ book title, word ]
 * Output value: DoubleWritable ---> TF-IDF Value
 */

public class CustomReducer extends Reducer<Text, TextArrayWritable, Text, DoubleWritable> {

    double totalBookCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalBookCount = context.getConfiguration().getDouble("Total.Book.Count", 0);
//        throw new IOException("books:" + totalBookCount);
    }


    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        Collection<TextArrayWritable> collection = new LinkedList<>();
        //calculate the number of books this word occurs
        for (TextArrayWritable val : values) {
            collection.add(val);
        }

        double bookOccurCount = collection.size();

        Iterator<TextArrayWritable> iterator = values.iterator();
        for (TextArrayWritable val : collection) {
            Text title = (Text) val.get()[0];
            Double wordCount = Double.parseDouble(val.get()[1].toString());
            Double maxWordCount = Double.parseDouble(val.get()[2].toString());
            //calculate TF-IDF value
            Double tf = wordCount / maxWordCount;
            Double idf = Math.log(totalBookCount / bookOccurCount) / Math.log(2);
            DoubleWritable tfidf = new DoubleWritable(tf / idf);

            String output = title.toString() + ":" + key.toString();
            Text outputKey = new Text(output);
            context.write(outputKey, tfidf);
        }
    }
}
