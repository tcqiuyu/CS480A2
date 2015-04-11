package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.input.CombineBooksInputFormat;
import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import cs480a2.yqiu.recSystem.mapreduce.tfidf.TFIDFCombiner;
import cs480a2.yqiu.recSystem.mapreduce.tfidf.TFIDFMapper;
import cs480a2.yqiu.recSystem.mapreduce.tfidf.TFIDFReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This class identify the configuration of Map reduce task.
 */

public class TFIDF {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //store an variable of total books count
        configuration.setDouble("Total.Book.Count", 0);
        Job job = Job.getInstance(configuration, "TF-IDF");


        //set main class
        job.setJarByClass(TFIDF.class);

        //set mapper/combiner/reducer
        job.setMapperClass(TFIDFMapper.class);
        job.setCombinerClass(TFIDFCombiner.class);
        job.setReducerClass(TFIDFReducer.class);

        //set the combine file size to maximum 64MB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        MultipleOutputs.addNamedOutput(job, "tfidf", TextOutputFormat.class, Text.class, TextArrayWritable.class);

        //set input path
        FileInputFormat.setInputPaths(job, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
//        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

    private void computeTF() throws IOException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "TF");

        job.setMapperClass(TFIDFMapper.class);
        job.set
    }

    private void computeIDF(){

    }
}
