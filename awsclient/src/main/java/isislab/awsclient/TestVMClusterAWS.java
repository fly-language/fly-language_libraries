package isislab.awsclient;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static ExecutorService __thread_pool_smp = Executors.newFixedThreadPool(4);
	static LinkedTransferQueue<Object> ch = new LinkedTransferQueue<Object>();
	static LinkedTransferQueue<Object> chTermination = new LinkedTransferQueue<Object>();
	static Boolean __wait_on_ch = true;
	static Boolean __wait_on_ch_termination = true;
	static 		long  __id_execution =  System.currentTimeMillis();

	
	static 		AmazonSQS __sqs_aws = AmazonSQSClientBuilder.standard()
			.withRegion("eu-west-2")							 
			.withCredentials(new AWSStaticCredentialsProvider(creds))
			.build();

	public static void main(String[] args) throws Exception {
		
		String region = "eu-west-2";
		String instance_type = "t2.micro";
		String purchasingOption = "on-demand";
		boolean persistent = false;
		int vmCount = 2;
		
		int smpThreadCount = 2;
		
		__sqs_aws.createQueue(new CreateQueueRequest("ch-termination-"+__id_execution));
		String terminationQueue = __sqs_aws.getQueueUrl("ch-termination-"+__id_execution).getQueueUrl();
		
		AWSClient awsClient = new AWSClient(creds, region, terminationQueue);
		
		//Upload current project on the S3 bucket created
		awsClient.zipAndUploadCurrentProject();
		
		//Run a cluster of VMs 	
		int vmsCreatedCount = awsClient.launchVMCluster(instance_type, purchasingOption, persistent,vmCount);
		
		if ( vmsCreatedCount != 0) {
			System.out.print("\n\u27A4 Waiting for virtual machines boot script to complete...");
			while ( Long.parseLong(__sqs_aws.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(terminationQueue)
											.withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages.toString())).getAttributes().get("ApproximateNumberOfMessages")) != vmsCreatedCount);
			System.out.println("Done");
		}
		if(vmsCreatedCount != vmCount){
			if ( vmsCreatedCount > 0) awsClient.downloadFLYProjectonVMCluster();
			
			System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
			while ( Long.parseLong(__sqs_aws.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(terminationQueue)
											.withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages.toString())).getAttributes().get("ApproximateNumberOfMessages")) != (vmCount+vmsCreatedCount));
		}
		System.out.println("Done");
		
		String mainClass = "sum";
		//Project Building
		awsClient.buildFLYProjectOnVMCluster(mainClass);
		
		System.out.print("\n\u27A4 Waiting for building project on VM CLuster to complete...");
		if(vmsCreatedCount != vmCount){
			while ( Long.parseLong(__sqs_aws.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(terminationQueue)
											.withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages.toString())).getAttributes().get("ApproximateNumberOfMessages")) != ( (vmCount*2)+vmsCreatedCount));
		} else {
			while ( Long.parseLong(__sqs_aws.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(terminationQueue)
											.withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages.toString())).getAttributes().get("ApproximateNumberOfMessages")) != (vmCount*2));
		}
		System.out.println("Done");
		
		int numberOfFunctions = 100;

		//Workload uniform splitting
		int[] dimPortions = new int[vmCount]; //size of each vm's portion
		int[] displ = new int[vmCount]; // start index of each vm's portion
		int offset = 0;
		for(int i=0; i<vmCount;i++){
		            dimPortions[i] = (numberOfFunctions / vmCount) +
		                            ((i < (numberOfFunctions % vmCount)) ? 1 : 0);
		            displ[i] = offset;
		            offset += dimPortions[i];
		}
		
		//results queue
		__sqs_aws.createQueue(new CreateQueueRequest("ch-"+__id_execution));
		for(int __i=0;__i< smpThreadCount;__i++){ 
			__thread_pool_smp.submit(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					while(__wait_on_ch) {
						ReceiveMessageRequest __recmsg = new ReceiveMessageRequest(__sqs_aws.getQueueUrl("ch-"+__id_execution).getQueueUrl()).
								withWaitTimeSeconds(1).withMaxNumberOfMessages(10);
						ReceiveMessageResult __res = __sqs_aws.receiveMessage(__recmsg);
						for(Message msg : __res.getMessages()) { 
							ch.put(msg.getBody());
							__sqs_aws.deleteMessage(__sqs_aws.getQueueUrl("ch-"+__id_execution).getQueueUrl(), msg.getReceiptHandle());
						}
					}
					return null;
				}
			});
		}
		
		String objectInputString = "test";
		awsClient.executeFLYonVMCluster(objectInputString,
										numberOfFunctions,
										__id_execution);
										
		
		System.out.print("\n\u27A4 Waiting for FLY execution to complete...");
		while (ch.size() != numberOfFunctions);
		__wait_on_ch = false;
		System.out.println("Done");
				
		totalSum();
		
		awsClient.deleteResourcesAllocated();
		__thread_pool_smp.shutdown();
		
		System.exit(0);
	}
	
	protected static  Object estimation()throws Exception{
		Integer sum = 0;
		
		Integer crt = 0;
		
		
		for(int i=0;i<100;i++){
			
			{
				
				sum += Integer.parseInt(ch.take().toString());
				
				crt += 1;
			}
		}
		
		System.out.println("pi estimation: " + (sum * 4.0) / crt);
		return null;
		}
	
	protected static  Object totalSum()throws Exception{
		Integer sum = 0;
		
		for(int i=0;i<100;i++){
			
			{
				
				sum += Integer.parseInt(ch.take().toString());
			}
		}
		
		System.out.println("sum: " + sum);
		return null;
		}

}
