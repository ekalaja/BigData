package EditDistance;

import java.io.*;
import java.net.URI;
import java.util.HashSet;

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
    public static class TableOneMapper extends
            Mapper<Object, Text, Text, Text> {

        private Configuration conf;
        private BufferedReader fis;
        private HashSet<String> tableTwo = new HashSet<String>();


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
                    String rowWord = null;
                    while ((rowWord = fis.readLine()) != null) {
                        tableTwo.add(rowWord);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '"
                            + StringUtils.stringifyException(ioe));
                }
                System.out.printf("SETUP IS COMPLETE " );
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String wordOne = value.toString();

            for (String wordTwo : tableTwo) {
                if (distance(wordOne, wordTwo) < 3) {
                    context.write(new Text(wordOne), new Text(wordTwo));
                }
            }
        }

        public static int distance(String s1, String s2) {
            int edits[][] = new int[s1.length() + 1][s2.length() + 1];
            for (int i = 0; i <= s1.length(); i++)
                edits[i][0] = i;
            for (int j = 1; j <= s2.length(); j++)
                edits[0][j] = j;
            for (int i = 1; i <= s1.length(); i++) {
                for (int j = 1; j <= s2.length(); j++) {
                    int u = (s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1);
                    edits[i][j] = Math.min(
                            edits[i - 1][j] + 1,
                            Math.min(
                                    edits[i][j - 1] + 1,
                                    edits[i - 1][j - 1] + u
                            )
                    );
                }
            }
            return edits[s1.length()][s2.length()];
        }

    }

    public static class TableTwoMapper extends
            Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString();
            context.write(new Text(record), new Text("two"));

        }

    }


    public static class ReduceJoinReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String wordOne = "";
            String wordTwo = "";
            for (Text t : values) {
                wordTwo = t.toString();
            }
            context.write(new Text(key), new Text(wordTwo));


        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "edit-distance");
        job.setJarByClass(Main
                .class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TableOneMapper.class);
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TableTwoMapper.class);
        job.addCacheFile(new Path(args[1]).toUri());
        Path outputPath = new Path(args[2]);


        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
