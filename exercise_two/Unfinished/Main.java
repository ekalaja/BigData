package EDistance;

import java.io.*;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Main {

    public static class TableOneMapper extends
            Mapper<Object, Text, Text, Text> {

//        private HashMap<String, String> tableTwo = new HashMap<String, String>();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String fullWord = value.toString();
            System.out.println("First mapper go<<<<<<<<<<<<<<<<");

            for (int i = 0; i < fullWord.length(); i++) {
                context.write(new Text(fullWord.charAt(i) + "" + fullWord.charAt(i)), new Text(fullWord + "\t" + "t1"));
            }
        }

    }


    public static class TableTwoMapper extends
            Mapper<Object, Text, Text, Text> {

//        private HashMap<String, String> tableTwo = new HashMap<String, String>();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String fullWord = value.toString();
            for (int i = 0; i < fullWord.length(); i++) {
                context.write(new Text(fullWord.charAt(i) + "" + fullWord.charAt(i)), new Text(fullWord + "\t" + "t2"));
            }
        }

    }

    public static class TableMapperFinal extends
            Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("\t");
            String[] valueParts = key.toString().split("\t");
            for (int i = 0; i < keyParts.length; i++) {
                for (int j = 0; j < valueParts.length; j++) {
                    context.write(new Text(keyParts[i] + "\t" + "t1"), new Text(valueParts[j]));
                }
            }
        }

    }

    public static class RemoveDuplicatesReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("REDUCER ONE INITIATED <<<<<<<<<<" + key);
            String wordsOfOne = "";
            String wordsOfTwo = "";
            for (Text t : values) {
                String[] parts = t.toString().split("\t");
                if (parts[1].equals("t1")) {
                    wordsOfOne = wordsOfOne + "\t" + parts[0];
                }
                if (parts[1].equals("t2")) {
                    wordsOfTwo = wordsOfTwo + "\t" + parts[0];
                }
            }
            context.write(new Text(wordsOfOne), new Text(wordsOfTwo));
//                }
        }

    }

    public static class FinalReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("REDUCING ROUND 2 BEGAN<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

            context.write(new Text(key), new Text("reduceROund2"));

        }

        public static int distance(String s1, String s2) {
            System.out.println("dist function calling WOOORDS " + s1 + " " + s2);
            int edits[][] = new int[s1.length() + 1][s2.length() + 1];
            for (int i = 0; i <= s1.length(); i++)
                edits[i][0] = i;
            for (int j = 1; j <= s2.length(); j++)
                edits[0][j] = j;
            for (int i = 1; i <= s1.length(); i++) {
                for (int j = 1; j <= s2.length(); j++) {
                    int u = (s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1);
                    int roundValue = Math.min(
                            edits[i - 1][j] + 1,
                            Math.min(
                                    edits[i][j - 1] + 1,
                                    edits[i - 1][j - 1] + u
                            )
                    );
                    if (roundValue > 2) {
                        return 100;
                    }
                }
            }
            return edits[s1.length()][s2.length()];
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "edit-distance");

        job.setJarByClass(Main
                .class);
        System.out.println("KÄYNTIINNNNNNNNNNNNNNNNNN");
        Path outputTemp = new Path("/output/Temp");

        Path outputPathFinal  = new Path(args[2]);
        outputPathFinal.getFileSystem(conf).delete(outputTemp);
//        job2.setInputFormatClass(SequenceFileInputFormat.class);



        job.setReducerClass(RemoveDuplicatesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TableOneMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TableTwoMapper.class);
        Path outputPath = new Path(args[2]);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputTemp);
        boolean jobOneReady = job.waitForCompletion(true);

        if (jobOneReady) {
            System.out.println("job2 KÄYNTIIN.....");
//            Job job2 = new Job(conf, "edit-distance-two");
            Job job2 = Job.getInstance(conf, "edit-distance-two");
            outputPathFinal.getFileSystem(conf).delete(outputPathFinal);
            job2.setMapperClass(TableMapperFinal.class);
            job2.setJarByClass(Main.class);
            job2.setReducerClass(FinalReducer.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
//            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            FileInputFormat.addInputPath(job2, outputTemp);
            FileOutputFormat.setOutputPath(job2, outputPathFinal);
            boolean jobTwoReady = job2.waitForCompletion(true);

            System.exit(jobTwoReady ? 0 : 1);

        }



//        FileOutputFormat.setOutputPath(job, outputPath);

    }

}