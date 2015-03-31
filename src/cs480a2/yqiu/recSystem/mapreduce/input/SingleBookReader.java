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
 * Output key is the current line
 * Output value is book title
 */
public class SingleBookReader extends RecordReader<Text, Text> {

    private LineReader lineReader;
    private Text currentLine = new Text(""); //key
    private Text title; //value

    private long start;
    private long end;
    private long currentPos;

    private String filename;

    private boolean hasTitle;
    private boolean hasStart;

    private Configuration configuration;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) inputSplit;
        configuration = context.getConfiguration();
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

        prepareToScanBook();
    }

    private void prepareToScanBook() {
        //get the title of the book
        while (!containsTitle(currentLine)) {
            try {
                int readBytes = lineReader.readLine(currentLine);
                //if does not find line of title, return
                if (readBytes == 0 || !hasTitle) {
                    hasTitle = false;
                    return;
                }
                //update cursor of linereader
                start += readBytes;
            } catch (IOException e) {
                hasTitle = false;
                System.err.println("Error when retriving title for book ---> " + filename);
                System.err.println(e.getMessage());
            }

        }

        //get book start line
        while (isBookStart(currentLine)) {
            try {
                int readBytes = lineReader.readLine(currentLine);
                //if does not find book start line, return
                if (readBytes == 0) {
                    hasStart = false;
                    return;
                }
                //update cursor of linereader
                start += readBytes;
            } catch (IOException e) {
                hasStart = false;
                System.err.println("Error when retriving start line for book ---> " + filename);
                System.err.println(e.getMessage());
            }
        }

    }

    private boolean containsTitle(Text line) {
        String lineString = line.toString();

        if (lineString.startsWith("Title")) {
            String titleString = lineString.split(":")[1].substring(1);
            title = new Text(titleString);
            return true;
        } else {
            return false;
        }
    }

    private boolean isBookStart(Text line) {
        String lineString = line.toString();
        return lineString.startsWith("*** START OF THIS PROJECT");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (filename.endsWith("txt")) {//only process txt file
            return false;
        }

        if (currentPos > end || !hasStart || !hasTitle) {//false if finishes processing the file
            return false;
        }

        //read next line
        int readBytes = lineReader.readLine(currentLine);
        currentPos += readBytes;

        if (currentLine.toString().startsWith("End of Project Gutenberg")) {
            double totalCount = configuration.getDouble("Total_Book_Count", 0);
            totalCount++;
            configuration.setDouble("Total_Book_Count", totalCount);
            return false;
        }

        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentLine;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return title;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}
