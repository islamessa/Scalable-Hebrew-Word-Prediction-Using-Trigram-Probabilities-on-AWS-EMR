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

public class Join {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
           // System.out.println(  "  MAPPER   read this :   "+line  );
            // Split the line into two parts: the key (words) and the values (numbers)
            String[] parts = line.split("\t", 2); // Split by tab, limit to 2 parts
            if (parts.length == 2) {
                String compositeKey = parts[0]; // The words (e.g., "הם הם תלמיד")
                String values = parts[1];      // The numbers (e.g., "264, 132, 44")
                //System.out.println(  " and the key is  :   "+compositeKey   );
                //System.out.println(  " and the value is  :   "+values   );
                // Tag values to distinguish list
            if (values.split(",").length == 4) {
                //System.out.println(  " and the send to the reducer   :   A|"+values   );
                context.write(new Text(compositeKey), new Text("A|" + values));
            } else if (values.split(",").length == 3) {
                //System.out.println(  " and the send to the reducer   :   B|"+values   );
                context.write(new Text(compositeKey), new Text("B|" + values));
            }}}
        }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] A = null;
            String[] B = null;
            //System.out.println(  "  reducer  read the key  :   "+key  );


            for (Text value : values) {
                //System.out.println(  "  reducer  read the value :   "+value  );
                String[] parts = value.toString().split("\\|");
                if (parts[0].equals("A")) {
                    //System.out.println(  "  reducer  read the value and its A "  );
                    A = parts[1].split(",");
                } else if (parts[0].equals("B")) {
                    //System.out.println(  "  reducer  read the value and its B "  );
                    B = parts[1].split(",");
                }
            }

            if (A != null && B != null) {
                // Parse A and B values
                double c0 = Double.parseDouble(A[0]);//c0
                double n1 = Double.parseDouble(A[1]);//c1
                double n2 = Double.parseDouble(A[2]);//c2
                double A4 = Double.parseDouble(A[3]);//tag
                double c1 = Double.parseDouble(B[0]);//n1
                double c2 = Double.parseDouble(B[1]);//n2
                double n3 = Double.parseDouble(B[2]);//n3

                //System.out.println(  "  reducer  read the value and its paresd to  " +c0+" "+c1+" "+c2+" "+n1+" "+n2+" "+n3  );
                // Calculate k2 and k3
                double k2 = (Math.log(n2 + 1) + 1) / (Math.log(n2 + 1) + 2);
                double k3 = (Math.log(n3 + 1) + 1) / (Math.log(n3 + 1) + 2);

                // Calculate the formula
                double result = k3 * (n3 / c2) + (1 - k3) * k2 * (n2 / c1) + (1 - k3) * (1 - k2) * (n1 / c0);
                //System.out.println(  "  output :   "+result  );

                context.write(key, new Text(String.valueOf(result)));
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
            return collator.compare(t1.toString(), t2.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Join");
        job.setJarByClass(Join.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(KeyComparator.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   }



