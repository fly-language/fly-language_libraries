package isislab.awsclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static HashMap<String,HashMap<String, Object>> __fly_environment = new HashMap<String,HashMap<String,Object>>();
	static long  __id_execution =  System.currentTimeMillis();
	static String terminationQueue;
	
	static ExecutorService __thread_pool_smp = Executors.newFixedThreadPool(4);
	static AWSClient aws = null;
	
	static AmazonSQS __sqs_aws = AmazonSQSClientBuilder.standard()
					.withRegion("eu-west-2")							 
					.withCredentials(new AWSStaticCredentialsProvider(creds))
					.build();
	static LinkedTransferQueue<Object> ch = new LinkedTransferQueue<Object>();
	static Boolean __wait_on_ch = true;
	static ExecutorService __thread_pool_vm_cluster = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	static Integer vmCount = 3;	
	static boolean __wait_on_termination_partialMatrix_0 = true;

	public static void main(String[] args) throws Exception{
		
		String region = "eu-west-2";
		String instance_type = "t3.micro";
		String purchasingOption = "on-demand";
		boolean persistent = false;
		int vmCount = 2;
		int smpThreadCount = 4;
		
		try {
			Integer [][] matrixOfIntegers = {{1,2,3,4,5,6,7,8,0},{1,2,3,4,5,6,7,8,1}};
			__wait_on_ch=false;
			__wait_on_termination_partialMatrix_0=true;
			__sqs_aws.createQueue(new CreateQueueRequest("termination-partialMatrix-"+__id_execution));
			LinkedTransferQueue<String> __termination_partialMatrix_ch_0  = new LinkedTransferQueue<String>();
			final String __termination_partialMatrix_url_0 = __sqs_aws.getQueueUrl("termination-partialMatrix-"+__id_execution).getQueueUrl();
			for(int __i=0;__i< (Integer)__fly_environment.get("smp").get("nthread");__i++){ 
				__thread_pool_smp.submit(new Callable<Object>() {
					@Override
					public Object call() throws Exception {
						while(__wait_on_termination_partialMatrix_0) {
							ReceiveMessageRequest __recmsg = new ReceiveMessageRequest(__termination_partialMatrix_url_0).
									withWaitTimeSeconds(1).withMaxNumberOfMessages(10);
							ReceiveMessageResult __res = __sqs_aws.receiveMessage(__recmsg);
							for(Message msg : __res.getMessages()) { 
								__termination_partialMatrix_ch_0.put(msg.getBody());
								__sqs_aws.deleteMessage(__termination_partialMatrix_url_0, msg.getReceiptHandle());
							}
						}
						return null;
					}
				});
			}
							
			__wait_on_ch=true;
			__sqs_aws.createQueue(new CreateQueueRequest("ch-"+__id_execution));
			
			for(int __i=0;__i< (Integer)__fly_environment.get("smp").get("nthread");__i++){ 
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
			
			aws = new AWSClient(creds,region, __termination_partialMatrix_url_0);
			
			int vCPUsCount_8 = aws.getVCPUsCount(instance_type);
			
			aws.zipAndUploadCurrentProject();
			
			int vmsCreatedCount_8 = aws.launchVMCluster(instance_type, purchasingOption, persistent, vmCount);
			
			if ( vmsCreatedCount_8 != 0) {
				System.out.print("\n\u27A4 Waiting for virtual machines boot script to complete...");
				while ( __termination_partialMatrix_ch_0.size() != vmsCreatedCount_8);
				System.out.println("Done");
			}
			if(vmsCreatedCount_8 != vmCount){
				if ( vmsCreatedCount_8 > 0) aws.downloadFLYProjectonVMCluster();
				
				System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
				while (__termination_partialMatrix_ch_0.size() != (vmCount+vmsCreatedCount_8));
			}
			System.out.println("Done");
			
			String mainClass_8 = "matrix_partialMatrix";
			aws.buildFLYProjectOnVMCluster(mainClass_8);
			
			System.out.print("\n\u27A4 Waiting for building project on VM CLuster to complete...");
			if(vmsCreatedCount_8 != vmCount){
				while ( __termination_partialMatrix_ch_0.size() != ( (vmCount*2)+vmsCreatedCount_8));
			} else {
				while (__termination_partialMatrix_ch_0.size() != (vmCount*2));
			}
			System.out.println("Done");			
			int splitCount_8 = vCPUsCount_8 * vmCount;
			int vmCountToUse_8 = vmCount;
			ArrayList<StringBuilder> __temp_matrixOfIntegers_8 = new ArrayList<StringBuilder>();
			ArrayList<String> portionInputs_8 = new ArrayList<String>();
			int __rows_8 = matrixOfIntegers.length;
			int __cols_8 = matrixOfIntegers[0].length;
			
			int __current_col_matrixOfIntegers_8 = 0;
			
			if ( __cols_8 < splitCount_8) splitCount_8 = __cols_8;
			if ( splitCount_8 < vmCountToUse_8) vmCountToUse_8 = splitCount_8;
										
			int[] dimPortions_8 = new int[splitCount_8]; 
			int[] displ_8 = new int[splitCount_8]; 
			int offset_8 = 0;
											
			for(int __i=0;__i<splitCount_8;__i++){
				dimPortions_8[__i] = (__cols_8 / splitCount_8) +
														((__i < (__cols_8 % splitCount_8)) ? 1 : 0);
				displ_8[__i] = offset_8;
				offset_8 += dimPortions_8[__i];
				
				__temp_matrixOfIntegers_8.add(__i,new StringBuilder());
				__temp_matrixOfIntegers_8.get(__i).append("{\"submatrixRows\":"+__rows_8+",\"submatrixCols\":"+dimPortions_8[__i]+",\"submatrixIndex\":"+__i+",\"submatrixDisplacement\":"+displ_8[__i]+",\"values\":[");							
				
				for(int __j = 0; __j<__rows_8;__j++){
					for(int __z=__current_col_matrixOfIntegers_8; __z<__current_col_matrixOfIntegers_8+dimPortions_8[__i];__z++){
						__temp_matrixOfIntegers_8.get(__i).append("{\"x\":"+__j+",\"y\":"+__z+",\"value\":"+matrixOfIntegers[__j][__z]+"},");
					}
					if(__j == __rows_8-1) {
						__temp_matrixOfIntegers_8.get(__i).deleteCharAt(__temp_matrixOfIntegers_8.get(__i).length()-1);
						__temp_matrixOfIntegers_8.get(__i).append("]}");
					}
				}
				__current_col_matrixOfIntegers_8+=dimPortions_8[__i];
				portionInputs_8.add(__generateString(__temp_matrixOfIntegers_8.get(__i).toString(),8));						
			}
			int numberOfFunctions_8 = splitCount_8;
			int notUsedVMs_8 = vmCount - vmCountToUse_8;
			
			//ALERT: the execution needs the other file to actually execute on the VM, so do it on FLY
			/*aws.executeFLYonVMCluster(portionInputs_8,
											numberOfFunctions_8,
											__id_execution);
			
			System.out.print("\n\u27A4 Waiting for FLY execution to complete...");
			if(vmsCreatedCount_8 != vmCount_1640273846582){
				while (__termination_partialMatrix_ch_0.size() != ( (vmCount_1640273846582*3)+vmsCreatedCount_8-notUsedVMs_8));
			} else {
				while (__termination_partialMatrix_ch_0.size() != (vmCount_1640273846582*3-notUsedVMs_8 ));
			}
			__wait_on_termination_partialMatrix_0=false;
			System.out.println("Done");
			
			//Check for execution errors
			String err_8 = aws.checkForExecutionErrors();
			if (err_8 != null) {
				//Print the error within each VM
				System.out.println("The execution failed with the following errors in each VM:");
				System.out.println(err_8);
			}else {
				//No execution errors
				//Manage the callback
				totalMatrix();
			}
			*/
			//Delete documents with commands
			aws.cleanResources();
			//Clear termination queue for eventual next iteration
			__termination_partialMatrix_ch_0.clear();
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			aws.deleteResourcesAllocated();
			
			__wait_on_ch=false;
			__sqs_aws.deleteQueue(new DeleteQueueRequest("ch-"+__id_execution));
			
			__sqs_aws.deleteQueue(new DeleteQueueRequest("termination-partialMatrix-"+__id_execution));
			
			__thread_pool_smp.shutdown();
			
			System.exit(0);
		}	
	}
	
	private static String __generateString(String s,int id) {
		StringBuilder b = new StringBuilder();
		b.append("{\"id\":"+id+",\"data\":");
		b.append("[");
		String[] tmp = s.split("\n");
		for(String t: tmp){
			b.append(t);
			if(t != tmp[tmp.length-1]){
				b.append(",");
			} 
		}
		b.append("]}");
		return b.toString();
	}

}
