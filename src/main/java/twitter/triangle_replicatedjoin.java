package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class triangle_replicatedjoin extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(triangle_replicatedjoin.class);
    public static final Integer MAX = 1000;

    public static class path2Mapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        public final String DELIMITER = ",";
        private Map<String, ArrayList<String>> edgeCache = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {

            try {
                Path edge_file_location=new Path(context.getConfiguration().get("edge.file.path") + '/' +
                        context.getConfiguration().get("edge.file.name"));//Location of file in HDFS
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(edge_file_location)));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] field = line.split(DELIMITER, -1);
                    String follower = field[0];
                    String followee = field[1];
                    if (Integer.parseInt(follower)<MAX && Integer.parseInt(followee)<MAX) {
                        edgeCache.computeIfAbsent(follower, k -> new ArrayList<>()).add(followee);
                    }
                }
                bufferedReader.close();
            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
                String msg = "Did not specify files in distributed cache1";
                logger.error(msg);
                throw new IOException(msg);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String R1follower = tokens[0];
            String R1followee = tokens[1];
            if (Integer.parseInt(R1follower)<MAX && Integer.parseInt(R1followee)<MAX) {
                // Ignore if the corresponding entry doesn't exist in the edge data (INNER JOIN)
                if (edgeCache.get(R1followee) != null) {
                    for (String R2followee : edgeCache.get(R1followee)) {
                        String path2 = R1follower + "_" + R2followee;
                        context.write(new Text(path2), one);
                    }
                }
            }
        }
    }

    public static class path2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class path3Mapper extends Mapper<Text, Text, Text, IntWritable> {
        public final String DELIMITER = ",";
        private Map<String, Integer> edgeCache = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            try {
                Path edge_file_location=new Path(context.getConfiguration().get("edge.file.path") + '/' +
                        context.getConfiguration().get("edge.file.name"));//Location of file in HDFS
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(edge_file_location)));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] field = line.split(DELIMITER, -1);
                    String follower = field[0];
                    String followee = field[1];
                    if (Integer.parseInt(follower)<MAX && Integer.parseInt(followee)<MAX) {
                        edgeCache.put(followee + '_' + follower, edgeCache.getOrDefault(followee + '_' + follower, 0) + 1);
                    }
                }
                bufferedReader.close();
            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
                String msg = "Did not specify files in distributed cache2";
                logger.error(msg);
                throw new IOException(msg);
            }
        }

        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            String path2pair = key.toString();
            Integer path2count = Integer.parseInt(value.toString());

            // record each edge
            if (edgeCache.get(path2pair) != null) {
                IntWritable path3count = new IntWritable(path2count * edgeCache.get(path2pair));
                context.write(new Text(path2pair), path3count);
            }
        }
    }

    public static class path3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public static final String TRIANGLE_COUNTER = "Triangle";

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.getCounter(TRIANGLE_COUNTER, "Number of triangle (not unique):").increment(val.get());
//                context.write(key, val);
            }
        }
    }

    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");
        String cachefilename = "edge_toy7.csv";

        // job one: join edge into path2
        final Configuration conf1 = getConf();
        conf1.set("edge.file.path", args[0]);
        conf1.set("edge.file.name", cachefilename);
        final Job job1 = Job.getInstance(conf1, "path2");
        job1.setJarByClass(triangle_replicatedjoin.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp1"));

        job1.setMapperClass(path2Mapper.class);
        job1.setReducerClass(path2Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        // job 2: check if path3 existed for each path2 pair
        final Configuration conf2 = getConf();
        conf2.set("edge.file.path", args[0]);
        conf2.set("edge.file.name", cachefilename);

        final Job job2 = Job.getInstance(conf2, "path3");
        job2.setJarByClass(triangle_replicatedjoin.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]+"/temp1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapperClass(path3Mapper.class);
        job2.setReducerClass(path3Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }
        }
        int code = job2.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            // Create a new file and write data to it.
            FileSystem fileSystem = FileSystem.get(conf1);
            Path path = new Path(args[1] + "/final/count.txt");
            FSDataOutputStream out = fileSystem.create(path);

            for (Counter counter : job2.getCounters().getGroup(path3Reducer.TRIANGLE_COUNTER)) {
                String fileContent = "Number of triangle (unique):" + '\t' + counter.getValue()/3;
                out.writeBytes(fileContent);
                System.out.println("Number of triangle (unique):" + '\t' + counter.getValue()/3);
            }
            // Close all the file descripters
            out.close();
            fileSystem.close();
        }
        return (code);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new triangle_replicatedjoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
        System.exit(0);
    }
}
