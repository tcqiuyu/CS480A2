package cs480a2.yqiu.recSystem.mapreduce.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This is the combine file input format
 * It combines multiple books into a larger chunk (64mb) as mapper's input
 * This is used for enhance performance.
 */

public class CombineBooksInputFormat extends CombineFileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return null;
    }
}
