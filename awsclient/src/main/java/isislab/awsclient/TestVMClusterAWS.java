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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static AWSClient aws = null;
	
	static long  __id_execution =  System.currentTimeMillis();
	
	static Integer vmCount = 2;	
	static Integer funcCount = 4;
	static Integer M = 500;	
	static Integer N = 500;	
	static Integer[] vector = new Integer[N];
	
	static ExecutorService __thread_pool_smp = Executors.newFixedThreadPool(4);

	static AmazonSQS __sqs_aws = AmazonSQSClientBuilder.standard()
			.withRegion("eu-west-2")							 
			.withCredentials(new AWSStaticCredentialsProvider(creds))
			.build();
	
	static boolean __wait_on_termination_matrixVectorMultiplication_0 = true;


	public static void main(String[] args) throws Exception{
		
		String region = "eu-west-2";
		String vmTypeSize_1643452623920 = "t2.micro";
		int vmCount_1643452623920 = 2;
		boolean persistent_1643452623920 = true;
		String purchasingOption_1643452623920 = "on-demand";

		try {
			/*
			String myConst = "[{\"name\":\"vector\",\"type\":\"Integer\",\"value\":[8, 2, 0, 6, 9, 9, 8, 0, 9, 8, 0, 1, 1, 8, 4, 7, 9, 0, 1, 9]}]";
			JSONArray jsonArray2 = new JSONArray(myConst);
			
			JSONArray JSONArrayValues = new JSONObject(jsonArray2.get(0).toString()).getJSONArray("value");
			System.out.println(JSONArrayValues.getInt(0));
			*/

			
			__wait_on_termination_matrixVectorMultiplication_0=true;
			__sqs_aws.createQueue(new CreateQueueRequest("termination-matrixVectorMultiplication-"+__id_execution));
			LinkedTransferQueue<String> __termination_matrixVectorMultiplication_ch_0  = new LinkedTransferQueue<String>();
			final String __termination_matrixVectorMultiplication_url_0 = __sqs_aws.getQueueUrl("termination-matrixVectorMultiplication-"+__id_execution).getQueueUrl();
			for(int __i=0;__i< 4;__i++){ 
				__thread_pool_smp.submit(new Callable<Object>() {
					@Override
					public Object call() throws Exception {
						while(__wait_on_termination_matrixVectorMultiplication_0) {
							ReceiveMessageRequest __recmsg = new ReceiveMessageRequest(__termination_matrixVectorMultiplication_url_0).
									withWaitTimeSeconds(1).withMaxNumberOfMessages(10);
							ReceiveMessageResult __res = __sqs_aws.receiveMessage(__recmsg);
							for(Message msg : __res.getMessages()) { 
								__termination_matrixVectorMultiplication_ch_0.put(msg.getBody());
								__sqs_aws.deleteMessage(__termination_matrixVectorMultiplication_url_0, msg.getReceiptHandle());
							}
						}
						return null;
					}
				});
			}
			
			aws = new AWSClient(creds,region);
			aws.setupS3Bucket("flybucketvmcluster");
			aws.setupTerminationQueue(__termination_matrixVectorMultiplication_url_0);
			
			int vCPUsCount_10 = aws.getVCPUsCount(vmTypeSize_1643452623920);
			
			aws.zipAndUploadCurrentProject();
					
			int vmsCreatedCount_10 = aws.launchVMCluster(vmTypeSize_1643452623920, purchasingOption_1643452623920, persistent_1643452623920, vmCount_1643452623920);
			
			if ( vmsCreatedCount_10 != 0) {
				System.out.print("\n\u27A4 Waiting for virtual machines boot script to complete...");
				while ( __termination_matrixVectorMultiplication_ch_0.size() != vmsCreatedCount_10);
				System.out.println("Done");
			}
			if(vmsCreatedCount_10 != vmCount_1643452623920){
				if ( vmsCreatedCount_10 > 0) aws.downloadFLYProjectonVMCluster();
				
				System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
				while (__termination_matrixVectorMultiplication_ch_0.size() != (vmCount_1643452623920+vmsCreatedCount_10));
			}
			System.out.println("Done");
			
			/*
			aws.downloadS3ObjectToFile("mySplitsi-02879fc2998be7349.txt");
			aws.downloadS3ObjectToFile("constValues.txt");
			
			FileInputStream fis = new FileInputStream("constValues.txt");       
			Scanner sc = new Scanner(fis);    //file to be scanned  
			
			int i =0;
			ArrayList<String> myConsts = new ArrayList<>();
			while(sc.hasNextLine()){ 
				String c = sc.nextLine();
				if (i == 0 && c.equals("None")) break;
				myConsts.add(c);
			}  
			sc.close();
			
			fis = new FileInputStream("mySplitsi-02879fc2998be7349.txt");       
			sc = new Scanner(fis);    //file to be scanned  
			
			ArrayList<String> mySplits = new ArrayList<>();
			int mySplitsCount = 0;
			i = 0;
			while(sc.hasNextLine()){
				String c = sc.nextLine(); 
				if (i == 0) mySplitsCount = Integer.parseInt(c);
				else mySplits.add(c);
			}
			sc.close();
			
			
			
			/*
			//input array example splitting
			int splitCount_33 = vCPUsCount_33 * vmCount_1643281687557;
			int vmCountToUse_33 = vmCount_1643281687557;
			ArrayList<StringBuilder> __temp_matrix_33 = new ArrayList<StringBuilder>();
			ArrayList<String> portionInputs_33 = new ArrayList<String>();
			
			int __rows_33 = matrix.length;
			int __cols_33 = matrix[0].length;
			
			int __current_row_matrix_33 = 0;
																
			if ( __rows_33 < splitCount_33) splitCount_33 = __rows_33;
			if ( splitCount_33 < vmCountToUse_33) vmCountToUse_33 = splitCount_33;
										
			int[] dimPortions_33 = new int[splitCount_33]; 
			int[] displ_33 = new int[splitCount_33]; 
			int offset_33 = 0;
										
			for(int __i=0;__i<splitCount_33;__i++){
				dimPortions_33[__i] = (__rows_33 / splitCount_33) + ((__i < (__rows_33 % splitCount_33)) ? 1 : 0);
				displ_33[__i] = offset_33;								
				offset_33 += dimPortions_33[__i];
											
				__temp_matrix_33.add(__i,new StringBuilder());
				__temp_matrix_33.get(__i).append("{\"portionRows\":"+dimPortions_33[__i]+",\"portionCols\":"+__cols_33+",\"portionIndex\":"+__i+",\"portionDisplacement\":"+displ_33[__i]+",\"portionValues\":[");							
					
				for(int __j=__current_row_matrix_33; __j<__current_row_matrix_33+dimPortions_33[__i];__j++){
					for(int __z = 0; __z<matrix[__j].length;__z++){
						__temp_matrix_33.get(__i).append("{\"x\":"+__j+",\"y\":"+__z+",\"value\":"+matrix[__j][__z]+"},");
					}
					if(__j == __current_row_matrix_33 + dimPortions_33[__i]-1) {
						__temp_matrix_33.get(__i).deleteCharAt(__temp_matrix_33.get(__i).length()-1);
						__temp_matrix_33.get(__i).append("]}");
					}
				}
				__current_row_matrix_33 +=dimPortions_33[__i];
				portionInputs_33.add(__generateString(__temp_matrix_33.get(__i).toString(),33));
				
			}
			int numberOfFunctions_33 = splitCount_33;
			int notUsedVMs_33 = vmCount_1643281687557 - vmCountToUse_33;
			
			aws.executeFLYonVMCluster(portionInputs_33,
					numberOfFunctions_33,
					__id_execution);
			
			aws.cleanResources();*/

			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			System.exit(0);
		}	
	}
	
	private static String __generateString(String s,int id) {
		StringBuilder b = new StringBuilder();
		b.append("{\"id\":\""+id+"\",\"data\":");
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
