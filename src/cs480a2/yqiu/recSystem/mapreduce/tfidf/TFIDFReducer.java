package cs480a2.yqiu.recSystem.mapreduce.tfidf;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Qiu on 3/18/15.
 * This is the reducer that calculate TF-IDF
 * Input key: Text ---> Word
 * Input value: TextArrayWritable ---> [ book title, word count, maximum word count for the book ]
 * Output key: TextArrayWritable ---> [ book title, word ]
 * Output value: DoubleWritable ---> TF-IDF Value
 */

public class TFIDFReducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

    MultipleOutputs<Text, TextArrayWritable> multipleOutputs;
    double totalBookCount;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        totalBookCount = context.getConfiguration().getDouble("Total.Book.Count", 600);
        totalBookCount = 600;
        multipleOutputs = new MultipleOutputs<>(context);
    }


    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        //this collection contains information for a certain word in each book that contains this word
        //information: [ book title that contains this word, word occurance, maximum word occurance for this book ]
//        HashMap<TextArrayWritable, Integer> map = new HashMap<>();
////
//        for (TextArrayWritable val : values) {
//            map.put(val, 0);
////            throw new IOException("Key: " + key + " --- Val: " + val.toString());
//        }

        //calculate the number of books this word occurs
//        double bookOccurCount = map.size();

//        Iterator<TextArrayWritable> iterator = values.iterator();
        for (TextArrayWritable val : values) {
//            Text title = (Text) val.get()[0];
//            Double wordCount = Double.parseDouble(val.get()[1].toString());
//            Double maxWordCount = Double.parseDouble(val.get()[2].toString());
//            //calculate TF-IDF value
//            Double tf = wordCount / maxWordCount;
//            Double idf = Math.log(totalBookCount / bookOccurCount) / Math.log(2);
//            DoubleWritable tfidf = new DoubleWritable(tf / idf);
//
//            String output = title.toString() + ":" + key.toString();
//            Text outputKey = new Text(output);
            context.write(key, val);
//            throw new IOException("Key: " + key + " --- Val: " +val);
//            multipleOutputs.write(key, val, "tfidf");

        }
        }
    }
