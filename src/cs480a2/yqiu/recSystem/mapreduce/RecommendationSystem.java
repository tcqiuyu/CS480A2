package cs480a2.yqiu.recSystem.mapreduce;

import cs480a2.yqiu.recSystem.mapreduce.bcv.BCVMapper;
import cs480a2.yqiu.recSystem.mapreduce.bcv.BCVReducer;
import cs480a2.yqiu.recSystem.mapreduce.input.CombineBooksInputFormat;
import cs480a2.yqiu.recSystem.mapreduce.similarity.SimilarityMapper;
import cs480a2.yqiu.recSystem.mapreduce.similarity.SimilarityReducer;
import cs480a2.yqiu.recSystem.mapreduce.structure.TextArrayWritable;
import cs480a2.yqiu.recSystem.mapreduce.tfidf.TFIDFMapper;
import cs480a2.yqiu.recSystem.mapreduce.tfidf.TFIDFReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Qiu on 3/18/15.
 * This class identify the configuration of Map reduce task.
 */

public class RecommendationSystem {

    public static Path firstTempPath = new Path("/output/tmp/1");
    public static Path secondTempPath = new Path("/output/tmp/2");
    public static Path thirdTempPath = new Path("/output/tmp/3");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();

        computeTFIDF(configuration, args);

        computeBCV(configuration);

        computeSimilarity(configuration);

    }

    private static void computeTFIDF(Configuration configuration, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //store an variable of total books count
        Job tfidf_Job = Job.getInstance(configuration, "TF-IDF");

        //set main class
        tfidf_Job.setJarByClass(RecommendationSystem.class);

        //set mapper/combiner/reducer
        tfidf_Job.setMapperClass(TFIDFMapper.class);
        tfidf_Job.setReducerClass(TFIDFReducer.class);

        //set the combine file size to maximum 64MB
        tfidf_Job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));
        tfidf_Job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);

        //set input path
        FileInputFormat.setInputPaths(tfidf_Job, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(tfidf_Job, true);
        tfidf_Job.setInputFormatClass(CombineBooksInputFormat.class);

        //set output path
        FileOutputFormat.setOutputPath(tfidf_Job, firstTempPath);

        tfidf_Job.setMapOutputKeyClass(Text.class);
        tfidf_Job.setMapOutputValueClass(TextArrayWritable.class);

        tfidf_Job.setOutputFormatClass(TextOutputFormat.class);
        tfidf_Job.setOutputKeyClass(Text.class);
        tfidf_Job.setOutputValueClass(TextArrayWritable.class);

        tfidf_Job.waitForCompletion(true);
    }

    private static void computeBCV(Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        Job bcv_Job = Job.getInstance(configuration, "BCV");

        bcv_Job.setJarByClass(RecommendationSystem.class);

        bcv_Job.setMapperClass(BCVMapper.class);
        bcv_Job.setReducerClass(BCVReducer.class);

        bcv_Job.setMapOutputKeyClass(Text.class);
        bcv_Job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(bcv_Job, firstTempPath);
        FileInputFormat.setInputDirRecursive(bcv_Job, true);
        bcv_Job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(bcv_Job, secondTempPath);

        bcv_Job.setOutputFormatClass(TextOutputFormat.class);

        bcv_Job.waitForCompletion(true);
    }

    private static void computeSimilarity(Configuration configuration) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Job similarity_Job = Job.getInstance(configuration, "EuclidD Job");

        similarity_Job.setJarByClass(RecommendationSystem.class);

        similarity_Job.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(similarity_Job, new Path("/output/tmp/2/part-r-00000"));

        // Each line of the file gets sent to a mapper
        similarity_Job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

        System.out.println("SETUP NLINEINPUT FORMAT!");

        similarity_Job.setMapperClass(SimilarityMapper.class);

        similarity_Job.setMapOutputKeyClass(Text.class);
        similarity_Job.setMapOutputValueClass(Text.class);

        similarity_Job.setReducerClass(SimilarityReducer.class);

        Path fourthJobInputPath = new Path("/output/tmp/2/part-r-00000");
        similarity_Job.addCacheFile(new URI("/output/tmp/2/part-r-00000#bcv"));

        Path fourthJobOutputPath = new Path("/output/tmp/3");

        FileInputFormat.setInputPaths(similarity_Job, fourthJobInputPath);
        FileOutputFormat.setOutputPath(similarity_Job, fourthJobOutputPath);

        similarity_Job.waitForCompletion(true);
    }

}
