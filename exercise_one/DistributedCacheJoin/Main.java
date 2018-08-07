package DistributedCacheJoin;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Main {
    public static class StudentMapper extends
            Mapper<Object, Text, Text, Text> {

        private Configuration conf;
        private BufferedReader fis;
        private HashMap<String, String> scoreDetails = new HashMap<String, String>();


        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            URI[] scoreURIs = Job.getInstance(conf).getCacheFiles();
            for (URI scoreURI : scoreURIs) {
                Path scorePath = new Path(scoreURI.getPath());
                String scoreFileName = scorePath.getName();
                try {
                    fis = new BufferedReader(new FileReader(scoreFileName));
                    String scoreRow = null;
                    while ((scoreRow = fis.readLine()) != null) {
                        String[] parts = scoreRow.split(",");
                        if (Integer.parseInt(parts[1]) > 80 && Integer.parseInt(parts[2]) < 96) {
                            scoreDetails.put(parts[0], parts[1] + "," + parts[2] + "," + parts[3]);
                        }
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '"
                            + StringUtils.stringifyException(ioe));
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");

            if (scoreDetails.containsKey(parts[0]) && Integer.parseInt(parts[2]) > 1990) {
                String[] scores = scoreDetails.get(parts[0]).split(",");
                String str = "," + parts[1] + "," + parts[2] + "," + scores[0] + "," + scores[1] + "," + scores[2];
                context.write(new Text(parts[0]), new Text(str));
            }
        }

    }

    public static class ReduceJoinReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text t : values) {
                context.write(new Text(key), new Text(t.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Distributed-Cache-Join");
        job.setJarByClass(Main
                .class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);

        job.addCacheFile(new Path(args[1]).toUri());
        Path outputPath = new Path(args[2]);


        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
