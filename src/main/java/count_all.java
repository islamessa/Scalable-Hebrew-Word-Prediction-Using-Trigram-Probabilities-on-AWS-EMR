import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class count_all {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text keyOutput = new Text();
        private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
                "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ",
                "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים",
                "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו",
                "הרבה", "הנה", "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה",
                "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו",
                "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול",
                "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת",
                "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
                "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה",
                "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא",
                "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
        ));

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().trim().split("\\t");

            // Ensure the line has at least 4 tokens (ngram, year, match_count, volume_count)
            if (tokens.length < 4) {
                return;
            }

            String ngram = tokens[0];
            String[] words = ngram.split("\\s+");
            int matchCount;

            try {
                matchCount = Integer.parseInt(tokens[2]);
            } catch (NumberFormatException e) {
                return; // Skip if match count is not a valid number
            }
            IntWritable ans =    new IntWritable(matchCount);
            // Ensure the line contains exactly three words

            if (words.length == 3) {
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                // Check if any word is in the stop words list
                if (STOP_WORDS.contains(w1) || STOP_WORDS.contains(w2) || STOP_WORDS.contains(w3)) {
                    return;
                }

                // Key 0: all words
                keyOutput.set("3-gram line");
                context.write(keyOutput, ans);
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum)); // Emit intermediate sum for each key
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
        private long count_all_words = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            String keyStr = key.toString();
                long sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                count_all_words = sum*3;
                context.write(new Text("total words:"), new Text(count_all_words+""));

            }
        }


    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
                return key.hashCode() % numPartitions;
            }
        }





    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println("the args[0] is : "+args[0]);
        System.out.println("and ouargs[1] is : "+args[1]);
        if (args.length >= 3) {
            System.out.println("and ouargs[2] is : "+args[2]);}


        //System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(count_all.class);

        job.setMapperClass(count_all.MapperClass.class);
        job.setPartitionerClass(count_all.PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(count_all.ReducerClass.class);
        job.setNumReduceTasks(1);
        //job.setSortComparatorClass(.KeyComparator.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       // job.setInputFormatClass(SequenceFileInputFormat.class);//  for big data only !!!!!!

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        System.out.println("the in is : "+args[0]);
        System.out.println("and out is : "+args[1]);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
