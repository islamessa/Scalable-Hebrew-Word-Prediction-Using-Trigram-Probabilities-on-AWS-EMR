import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.Random;

public class App_all {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 5;

    public static void main(String[]args){
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

//- The path to the Hebrew 3-Gram in S3:
//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data
        // the real input
        Random rand = new Random();
        int x = rand.nextInt(1000);
       // String he_3_gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
        String he_3_gram ="s3://hadoop211963863/arbix-new.txt";
        //String forSorting="s3://hadoop211963863/textToSort.txt";
        String output_step1 = "s3://hadoop211963863/outputs/count_all/" + x;
        String output_step3 = "s3://hadoop211963863/outputs/count_w3/" + x;
        String output_step2 = "s3://hadoop211963863/outputs/count_w2/" + x;
        String output_step4 = "s3://hadoop211963863/outputs/Join/" + x;
        String output_step5 = "s3://hadoop211963863/outputs/sort/" + x;

        // Step 1 - > all
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://hadoop211963863/jars/count_allS.jar") // chinge jar when is done
                .withArgs(he_3_gram, output_step1)// args [0] ,  args [1]
                .withMainClass("Step1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        // STEP 3 -> count w3 + send all
        HadoopJarStepConfig step3 = new HadoopJarStepConfig ()
                .withJar("s3://hadoop211963863/jars/count_w3S.jar")
                .withArgs(he_3_gram, output_step1,output_step3)
                .withMainClass("Step3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

// STEP 2 ->  count_w2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig ()
                .withJar("s3://hadoop211963863/jars/count_w2S.jar")
                .withArgs(he_3_gram, output_step2,output_step1)
                .withMainClass("Step2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
// STEP  4 - > join  count_w2 and count_w3
        HadoopJarStepConfig step4 = new HadoopJarStepConfig ()
                .withJar("s3://hadoop211963863/jars/Join.jar")
                .withArgs(output_step2, output_step3, output_step4)
                .withMainClass("Step4");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                .withJar("s3://hadoop211963863/jars/sort.jar") // chinge jar when is done
                .withArgs(output_step4, output_step5)// args [1] ,  args [2]
                .withMainClass("Step5");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3,stepConfig4,stepConfig5)
                .withLogUri("s3://hadoop211963863/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}
