import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

public class sort {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Split the key (entire line) into parts
            String[] parts = key.toString().split("\\s+");

            // Extract the first three words and the number
            if (parts.length >= 4) { // Ensure the line has at least three words and a number
                String firstThreeWords = parts[0] + " " + parts[1] + " " + parts[2];
                double number = Double.parseDouble(parts[parts.length - 1]);

                // Output the first three words as the key and the number as the value
                context.write(new Text(firstThreeWords), new Text(""+number));
            }
        }
    }
    // in the first step you need to send by the secand word
    // and  in the secand step the same but with w3
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;


        }
    }


    // this is how to comper hebrow words ???
    public static class KeyComparator extends WritableComparator {
        private Collator collator;

        protected KeyComparator() {
            super(Text.class, true);
            collator = Collator.getInstance(new Locale("he")); // Hebrew locale
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text t1 = (Text) w1;
            Text t2 = (Text) w2;

            String[] parts1 = t1.toString().split("\\s+");
            String[] parts2 = t2.toString().split("\\s+");

            // Compare first two words
            int wordComparison = (parts1[0] + " " + parts1[1]).compareTo(parts2[0] + " " + parts2[1]);
            if (wordComparison != 0) {
                return wordComparison;
            }

            // Compare the number at the end of the line
            double num1 = Double.parseDouble(parts1[parts1.length - 1]);
            double num2 = Double.parseDouble(parts2[parts2.length - 1]);
            return -Double.compare(num1, num2);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(sort.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setNumReduceTasks(1); // Ensure only one reducer is used


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   }




