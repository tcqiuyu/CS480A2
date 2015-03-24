package cs480a2.yqiu.recSystem.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by Qiu on 3/18/15.
 * This class identify the configuration of Map reduce task.
 */

public class MainClass {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "TF_IDF");

        //set main class
        job.setJarByClass(MainClass.class);

        //set mapper/combiner/reducer
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomCombiner.class);
        job.setReducerClass(CustomReducer.class);

        //set the combine file size to maximum 64MB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);


    }
}
