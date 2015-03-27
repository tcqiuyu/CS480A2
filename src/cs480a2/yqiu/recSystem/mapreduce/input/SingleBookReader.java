package cs480a2.yqiu.recSystem.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This record reader takes a single book as input.
 * Output key is the word
 * Output value is book title
 */
public class SingleBookReader extends RecordReader<Text, Text> {

    private LineReader lineReader;
    private Text word; //key
    private Text title; //value

    private long start;
    private long end;
    private long currentPos;

    private String filename;
    private Text currentLine = new Text("");

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();
        Path path = split.getPath();
        filename = path.getName();
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        lineReader = new LineReader(inputStream, configuration);

        //initial start point and end point
        start = split.getStart();
        end = start + split.getLength();

        inputStream.seek(start);
        if (start != 0) {
            start += lineReader.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }

        start += lineReader.readLine(currentLine);


    }

    private void prepareToScanBook() {
        while (!getTitle(currentLine)) {
            try {
                int readBytes = lineReader.readLine(currentLine);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        while ()
    }

    private boolean getTitle(Text line) {
        String lineString = line.toString();

        if (lineString.startsWith("Title")) {
            String titleString = lineString.split(":")[1].substring(1);
            title = new Text(titleString);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}
