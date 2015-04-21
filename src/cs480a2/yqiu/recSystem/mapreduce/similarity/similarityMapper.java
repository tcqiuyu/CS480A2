package cs480a2.yqiu.recSystem.mapreduce.similarity;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by yqiu on 3/23/15.
 * Input Key: LongWritable ---> Text Offset
 * Input Value: Text ---> "Title    (BCV)" ---> "Title  Word1=TFIDF1 Word2=TFIDF2 ..."
 * Output Key:
 */
public class SimilarityMapper extends Mapper {
}
