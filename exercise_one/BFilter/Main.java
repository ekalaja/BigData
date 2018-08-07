package BFilter;

import java.io.*;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class Main {
    public static class StudentMapper extends
            Mapper<Object, Text, Text, Text> {

        private BloomFilter filter = new BloomFilter();
        private Configuration conf;


        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            File f = new File("bfile");
            FileInputStream fis = new FileInputStream(f);
            DataInputStream dis = new DataInputStream(fis);

            filter.readFields(dis);
            dis.close();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split(",");

            if (filter.membershipTest(new Key(parts[0].getBytes()))) {
                if (Integer.parseInt(parts[2]) > 1990) {
                    context.write(new Text(parts[0]), new Text("stu\t" + parts[1] + "\t" + parts[2]));
                }
            }
        }
    }

    public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            if (Integer.parseInt(parts[1]) > 80 && Integer.parseInt(parts[2]) < 96) {
                context.write(new Text(parts[0]), new Text("sco\t" + parts[1] + "\t" + parts[2] + "\t" + parts[3]));
            }
        }
    }

    public static class ReduceJoinReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            String year = "";
            String scoreOne = "";
            String scoreTwo = "";
            String scoreThree = "";

            for (Text t : values) {
                String parts[] = t.toString().split("\t");
                if (parts[0].equals("sco")) {
                    scoreOne = parts[1];
                    scoreTwo = parts[2];
                    scoreThree = parts[3];
                } else if (parts[0].equals("stu")) {
                    name = parts[1];
                    year = parts[2];
                }
            }
            String str = "," + name + "," + year + "," + scoreOne + "," + scoreTwo + "," + scoreThree;

            if (name.length() > 0 && year.length() > 0 && scoreOne.length() > 0) {
                context.write(new Text(key), new Text(str));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "bfilter");
        job.setJarByClass(Main
                .class);
        Date date = new Date();
        System.out.println("Time is: " + date.toString());
        BloomFilter bFilt = new BloomFilter(18000000, 3,
                Hash.MURMUR_HASH);
        Path bloomOP = new Path("output/bfile");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(bloomOP)) {
            fs.delete(bloomOP, true);
        }

        InputStreamReader iSR = new InputStreamReader(fs.open(new Path(args[1])));
        BufferedReader br = new BufferedReader(iSR, 1024);
        String line;

        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            if (Integer.parseInt(parts[1]) > 80 && Integer.parseInt(parts[2]) < 96) {
                bFilt.add(new Key(parts[0].getBytes()));
            }
        }
        iSR.close();
        br.close();
        FSDataOutputStream fSD = fs.create(bloomOP);

        bFilt.write(fSD);

        fSD.flush();
        fSD.close();
        System.out.println("BLOOMFILTER CREATED <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        date = new Date();
        System.out.println("Time is: " + date.toString());

        job.setInputFormatClass(TextInputFormat.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(bloomOP.toUri());
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);

        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

