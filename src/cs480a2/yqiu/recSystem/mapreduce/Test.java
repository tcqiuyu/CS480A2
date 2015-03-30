package cs480a2.yqiu.recSystem.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Test {

    //part1------------------------------------------------------------------------

    public static class Mapper_Part1 extends Mapper<LongWritable, Text, Text, Text> {
        String File_name = ""; //�����ļ����������ļ������������ļ�
        int all = 0;  //��������ͳ��
        static Text one = new Text("1");
        String word;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String str = split.getPath().toString();
            File_name = str.substring(str.lastIndexOf("/") + 1); //��ȡ�ļ���
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = File_name;
                word += " ";
                word += itr.nextToken();  //���ļ����ӵ�����Ϊkey es: test1 hello  1
                all++;
                context.write(new Text(word), one);
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            //Map��������ǽ����ʵ�����д�롣������Ҫ���ܵ����������㡣
            String str = "";
            str += all;
            context.write(new Text(File_name + " " + "!"), new Text(str));
            //��Ҫ����ֵʹ�õ� "!"���ر���ġ� ��Ϊ!��ascii�����е���ĸ��С��
        }
    }

    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
        float all = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int index = key.toString().indexOf(" ");
            //��Ϊ!��ascii��С��������map�׶ε������!������ڵ�һ��
            if (key.toString().substring(index + 1, index + 2).equals("!")) {
                for (Text val : values) {
                    //��ȡ�ܵĵ�������
                    all = Integer.parseInt(val.toString());
                }
                //���key-value������
                return;
            }
            float sum = 0;  //ͳ��ĳ�����ʳ��ֵĴ���
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            //����ѭ����ĳ�����������ֵĴ�����ͳ�����ˣ����� TF(��Ƶ) = sum / all
            float tmp = sum / all;
            String value = "";
            value += tmp;  //��¼��Ƶ

            //��key�е��ʺ��ļ������л�����es: test1 hello -> hello test1
            String p[] = key.toString().split(" ");
            String key_to = "";

            key_to += p[1];
            key_to += " ";
            key_to += p[0];

            context.write(new Text(key_to), new Text(value));
        }
    }

    public static class MyPartitoner extends Partitioner<Text, Text> {
        //ʵ���Զ����Partitioner
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            //���ǽ�һ���ļ��м���Ľ����Ϊһ���ļ�����
            //es�� test1 test2
            String ip1 = key.toString();
            ip1 = ip1.substring(0, ip1.indexOf(" "));
            Text p1 = new Text(ip1);
            return Math.abs((p1.hashCode() * 127) % numPartitions);
        }
    }

    public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {

                context.write(key, val);
            }
        }

    }

    //part2-----------------------------------------------------
    public static class Mapper_Part2 extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().replaceAll("	", " "); //��vlaue�е�TAB�ָ�����ɿո� es: Bank test1	0.11764706 -> Bank test1 0.11764706
            int index = val.indexOf(" ");
            String s1 = val.substring(0, index); //��ȡ���� ��Ϊkey es: hello
            String s2 = val.substring(index + 1); //���ಿ�� ��Ϊvalue es: test1 0.11764706
            s2 += " ";
            s2 += "1";  //ͳ�Ƶ��������������г��ֵĴ���, ��1�� ��ʾ����һ�Ρ� es: test1 0.11764706 1
            context.write(new Text(s1), new Text(s2));
        }
    }

    public static class Reduce_Part2 extends
            Reducer<Text, Text, Text, Text> {
        int file_count;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //ͬһ�����ʻᱻ�ֳ�ͬһ��group
            file_count = context.getNumReduceTasks();  //��ȡ���ļ���
            float sum = 0;
            List<String> vals = new ArrayList<String>();
            for (Text str : values) {
                int index = str.toString().lastIndexOf(" ");
                sum += Integer.parseInt(str.toString().substring(index + 1)); //ͳ�ƴ˵����������ļ��г��ֵĴ���
                vals.add(str.toString().substring(0, index)); //����
            }
            float tmp = sum / file_count;  //�����������ļ��г��ֵĴ����������ļ��� = DF
            for (int j = 0; j < vals.size(); j++) {
                String val = vals.get(j);
                String end = val.substring(val.lastIndexOf(" "));
                float f_end = Float.parseFloat(end); //��ȡTF
                val += " ";
                val += tmp;
                val += " ";
                val += f_end / tmp;  // f_end / tmp = tf-idfֵ
                context.write(key, new Text(val));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Path tmp = new Path("tmp"); //�����м��ļ���ʱ�洢Ŀ¼
        //part1----------------------------------------------------
        Configuration conf1 = new Configuration();
        //�����ļ��������ڼ���DF(�ļ�Ƶ��)ʱ��ʹ��
        FileSystem hdfs = FileSystem.get(conf1);
        FileStatus p[] = hdfs.listStatus(new Path(args[1]));

        //��ȡ�����ļ������ļ��ĸ�����Ȼ��������NumReduceTasks
        Job job1 = new Job(conf1, "My_tdif_part1");

        job1.setJarByClass(Test.class);

        job1.setMapperClass(Mapper_Part1.class);
        job1.setCombinerClass(Combiner_Part1.class); //combiner�ڱ���ִ�У�Ч��Ҫ�ߵ㡣
        job1.setReducerClass(Reduce_Part1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setNumReduceTasks(p.length);

        job1.setPartitionerClass(MyPartitoner.class); //ʹ���Զ���MyPartitoner

        FileInputFormat.addInputPath(job1, new Path(args[1]));

        FileOutputFormat.setOutputPath(job1, tmp);

        job1.waitForCompletion(true);
        //part2----------------------------------------
        Configuration conf2 = new Configuration();

        Job job2 = new Job(conf2, "My_tdif_part2");

        job2.setJarByClass(Test.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Mapper_Part2.class);
        job2.setReducerClass(Reduce_Part2.class);
        //��Ҫ�����£�������û��ʹ���Զ���Partitioner,Ĭ�ϵ�Partitioner�����key�����֣���������
        //����Ҫ���ַ�ʽ���������ļ���ͬһ�����ʻ�Ϊͬһ���飬��������ͳ��һ�������������ļ��г��ֵĴ�����
        job2.setNumReduceTasks(p.length);

        FileInputFormat.setInputPaths(job2, tmp);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        hdfs.delete(tmp, true);
    }
}
