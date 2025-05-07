import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
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
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;




import java.io.*;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class count_w3 {

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
               // keyOutput.set("*"+" "+"*"+" "+"*");
                //context.write(keyOutput, ans);

                // Key 1: one word
                keyOutput.set("*" + " " + "*" + " " + w1);
                context.write(keyOutput, ans);
                keyOutput.set("*" + " " + "*" + " " + w2);
                context.write(keyOutput, ans);
                keyOutput.set("*" + " " + "*" + " " + w3);
                context.write(keyOutput, ans);

                // Key 2: First word + Second word

                keyOutput.set("*"+" " + w2 + " " + w3);
                context.write(keyOutput, ans);

                keyOutput.set("*"+" " + w1 + " " + w2);
                context.write(keyOutput, ans);

                // Key 3: First word + Second word + Third word
                keyOutput.set(w1 + " " + w2 + " " + w3);
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
        private long star_star_word = 0;
        private long star_word_word = 0;
        public  static AmazonS3 S3;
        public  static   int C0;

        public static String extractFilePathFromS3Uri(String s3Uri, String bucketName) {
            // Check if the URI starts with "s3://bucketName/"
            StringBuilder sb = new StringBuilder(s3Uri);

            // Create the prefix to remove: "s3://bucket-name/"
            String prefix = "s3://" + bucketName + "/";

            // Ensure the URI starts with the expected prefix
            if (s3Uri.startsWith(prefix)) {
                // Remove the prefix (bucket name part) from the URI
                sb.delete(0, prefix.length());
            }

            // Return the remaining path after the bucket name
            return sb.toString();
        }

        protected void setup(Context context) throws IOException, InterruptedException { // need to be tsested
            super.setup(context);
            

         String outputStep1 = context.getConfiguration().get("count_all");

            String s3Uri = outputStep1+"/part-r-00000";
            String bucketName = "hadoop211963863";

            // Extract file path after bucket name using StringBuilder
            String filePath = extractFilePathFromS3Uri(s3Uri, bucketName);
            // Print the result
            System.out.println(filePath);
            try {
                S3 = AmazonS3ClientBuilder.standard()
                        .withCredentials(DefaultAWSCredentialsProviderChain.getInstance()) // Default credentials provider
                        .withRegion("us-west-2") // Ensure this matches your bucket region
                        .build();
                // Create a GetObjectRequest
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, filePath);


                // Download the file as an InputStream
                InputStream inputStream = S3.getObject(getObjectRequest).getObjectContent();

                // Read the file line by line using BufferedReader
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line); // Process the line as needed
                        if (line != null && line.startsWith("total words:")) {
                    String[] parts = line.split(":");
                    context.getConfiguration().set("count_all_words",parts[1].trim());
                } else {
                    throw new IllegalArgumentException("Invalid file format");
                }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error occurred: " + e.getMessage());
            }
            // Fetch the file path passed from the main class (Step 3)
//            File file = new File(outputStep1+"/part-r-00000");
//            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//                String line = br.readLine(); // Read the first line
//                if (line != null && line.startsWith("total words:")) {
//                    String[] parts = line.split(":");
//                    context.getConfiguration().set("count_all_words",parts[1].trim());
//                } else {
//                    throw new IllegalArgumentException("Invalid file format");
//                }
//            }
          }
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            String keyStr = key.toString();
            String count_all_words = context.getConfiguration().get("count_all_words");

            /*if(keyStr.matches("\\*\\s+\\*\\s+\\*")){
                long sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                count_all_words = sum*3;
                context.write(new Text("* * *"), new Text(count_all_words+""));
             */  // } else {
                    long sum = 0;
                    for (IntWritable value : values) {
                        sum += value.get();
                    }
                    if (keyStr.matches("\\*\\s+\\*\\s+\\S+")) {
                        star_star_word = sum;
                    } else if (keyStr.matches("\\*\\s+\\S+\\s+\\S+")) {
                        star_word_word = sum;
                    } else if (keyStr.matches("\\S+\\s+\\S+\\s+\\S+")) {
                        context.write(new Text(keyStr), new Text(count_all_words + ", " +star_star_word + ", " + star_word_word + ", " + sum ));
                    }
           // }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
//            String keyStr = key.toString();
//            if(keyStr.matches("\\*\\s+\\*\\s+\\*")){
//                return 0;
//            } else {
//                String[] words = keyStr.split("\\s+");
//                int partition_number = words[2].hashCode() % numPartitions;
//                if(partition_number == 0) // the purpose is to let the word total count alone in partition 0
//                    return partition_number+1;
//                else
//                    return partition_number;
//                //return key.hashCode() % numPartitions;
            String keyStr = key.toString();
            String[] words = keyStr.split("\\s+");
            int partition_number = (words[2].hashCode()& Integer.MAX_VALUE) % numPartitions;
            return partition_number;
             }
        }


    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;

            // Split the keys into words
            String[] words1 = key1.toString().split(" ");
            String[] words2 = key2.toString().split(" ");

            // Ensure keys have exactly three words
            if (words1.length < 3 || words2.length < 3) {
                return key1.compareTo(key2); // Default to lexicographical comparison
            }

            // Sort by the third word
            int cmp = words1[2].compareTo(words2[2]);
            if (cmp != 0) return cmp;

            // Then sort by the second word
            cmp = words1[1].compareTo(words2[1]);
            if (cmp != 0) return cmp;

            // Finally, sort by the first word
            return words1[0].compareTo(words2[0]);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        String count_all = args[2];
        conf.set("count_all", count_all);// the file

        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(count_w3.class);

        job.setMapperClass(count_w3.MapperClass.class);
        job.setPartitionerClass(count_w3.PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(count_w3.ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//job.setInputFormatClass(SequenceFileInputFormat.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
