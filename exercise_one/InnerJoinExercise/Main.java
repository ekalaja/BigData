package InnerJoinExercise;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static class StudentMapper extends
            Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            if (Integer.parseInt(parts[2]) > 1990) {
            context.write(new Text(parts[0]), new Text("stu\t" + parts[1] + "\t" + parts[2]));
            }
        }
    }

    public static class ScoreMapper extends
            Mapper<Object, Text, Text, Text> {
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
                    //total += Float.parseFloat(parts[1]);
                } else if (parts[0].equals("stu")) {
                    name = parts[1];
                    year = parts[2];
                }
            }
            String str = "," + name + "," + year + "," + scoreOne + "," + scoreTwo + "," + scoreThree;

            if (name.length() > 0 && year.length() > 0 && scoreOne.length() > 0) {
//                if (Integer.parseInt(scoreOne) > 80 && Integer.parseInt(scoreTwo) < 96 && Integer.parseInt(year) > 1990) {
                context.write(new Text(key), new Text(str));
//                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "inner-join");
        job.setJarByClass(Main
                .class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ScoreMapper.class);
        Path outputPath = new Path(args[2]);


        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
