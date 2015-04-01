package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * Mapper to calculate TF-IDF
 * Input: Text, Text ---> line, title
 * Output: Text, TextArrayWritable ---> title, [ word, "1", "1" ]
 * First "1": Will be accumulated as freq of this word
 * Second "1": Will be accumulated as all words count in given book
 */

public class CustomMapper extends Mapper<Text, Text, Text, TextArrayWritable> {

    private static final Text one = new Text("1");
    private Context context;

    private Text title;


    @Override
    public void map(Text currentSentence, Text value, Context context) throws IOException, InterruptedException {
        this.context = context;
        this.title = value;
//        throw new IOException("Key: " + entry.getKey() + "------------ Value: " + outVal);

        processSentence(currentSentence);
    }

    private void processSentence(Text sentence) throws IOException, InterruptedException {
        String sentenceStr = sentence.toString();
        String[] words = sentenceStr.split(" ");

        for (String word : words) {

            //replace all non-alphanumeric char
            word = word.trim().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

            if (isWord(word)) {
                Text wordText = new Text(word);
                Text[] textArray = {wordText, one, one};
                TextArrayWritable outputVal = new TextArrayWritable(textArray);
                context.write(title, outputVal);

            }
        }
    }

    private static boolean isWord(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return true;
        }
        return false;
    }
}
