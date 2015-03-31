package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This is the reducer that calculate TF-IDF
 * Input key: Text ---> Word
 * Input value: TextArrayWritable ---> [ book title, word count, word count for the book ]
 * Output key: TextArrayWritable ---> [ book title, word ]
 * Output value: DoubleWritable ---> TF-IDF Value
 */

public class CustomReducer extends Reducer<Text, TextArrayWritable, TextArrayWritable, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

        Double totalBookCount = context.getConfiguration().getDouble("Total_Book_Count", 0);
        Double bookOccurCount = 0.0;

        //calculate the number of books this word occurs
        for (TextArrayWritable val : values) {
            bookOccurCount++;
        }

        //output [ Word, Title ]
        for (TextArrayWritable val : values) {
            Text title = (Text) val.get()[0];
            Double wordCount = Double.parseDouble(val.get()[1].toString());
            Double maxWordCount = Double.parseDouble(val.get()[2].toString());
            //calculate TF-IDF value
            Double tf = wordCount / maxWordCount;
            Double idf = Math.log(totalBookCount / bookOccurCount) / Math.log(2);
            DoubleWritable tfidf = new DoubleWritable(tf / idf);

            TextArrayWritable outputKey = new TextArrayWritable(new Text[]{title, key});

            context.write(outputKey, tfidf);
        }
    }
}
