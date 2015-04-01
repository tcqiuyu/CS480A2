package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.input.CombineBooksInputFormat;
import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This class identify the configuration of Map reduce task.
 */

public class MainClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "TF-IDF");

        //store an variable to store total books
        configuration.setDouble("Total_Book_Count", 0);

        //set main class
        job.setJarByClass(MainClass.class);

        //set mapper/combiner/reducer
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomCombiner.class);
        job.setReducerClass(CustomReducer.class);


        //set the combine file size to maximum 64MB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        //set input path
        FileInputFormat.setInputPaths(job, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setSortComparatorClass(Text.Comparator.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
