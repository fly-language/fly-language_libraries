package isislab.azureclient;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

public class TestVMClusterAZURE {
	
	static ExecutorService __thread_pool_smp = Executors.newFixedThreadPool(4);
	static LinkedTransferQueue<Object> ch = new LinkedTransferQueue<Object>();
	static Boolean __wait_on_ch = true;
	static 		long  __id_execution =  System.currentTimeMillis();
	static AzureClient azure;
	
	public static void main(String[] args) throws Exception {

		String clientId = "";
		String tenantId = "";
		String secret = "";
		String subscriptionId = "";
		
		String region = "west europe";
		String vmSize = "Standard_B1s"; //Standard_A1_v2 as an alternative, be careful 4 vCPUs are allowed as max with Student Subscription
		String purchasingOption = "on-demand";
		int vmCount = 2;
		
		int smpThreadCount = 4;
		
		String terminationQueueName = "ch-termination-"+__id_execution;
		String resultsQueueName = "ch-"+__id_execution;

		azure = new AzureClient(clientId, tenantId, secret, subscriptionId,__id_execution+"", region, terminationQueueName);
		azure.VMClusterInit();
		
		azure.setupQueue(terminationQueueName);
		azure.setupQueue(resultsQueueName);

		//ZIP and upload project on Azure
		azure.zipAndUploadCurrentProject();
				
		int vmsCreatedCount = azure.launchVMCluster(vmSize, purchasingOption, false, vmCount);
		
		if ( vmsCreatedCount != 0) {
			System.out.print("\n\u27A4 Waiting for virtual machines boot script to complete...");
			while (azure.getQueueLength(terminationQueueName) != vmsCreatedCount);
			System.out.println("Done");
		}
		if(vmsCreatedCount != vmCount){
			if ( vmsCreatedCount > 0) azure.downloadFLYProjectonVMCluster();
			
			System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
			while (azure.getQueueLength(terminationQueueName) != (vmCount+vmsCreatedCount));
		}
		System.out.println("Done");
		
		azure.buildFLYProjectOnVMCluster();
		
		System.out.print("\n\u27A4 Waiting for building project on VM CLuster to complete...");
		if(vmsCreatedCount != vmCount){
			while (azure.getQueueLength(terminationQueueName) != ( (vmCount*2)+vmsCreatedCount));
		} else {
			while (azure.getQueueLength(terminationQueueName) != (vmCount*2));
		}
		System.out.println("Done");
		
		//Workload uniform splitting
		int[] dimPortions = new int[vmCount]; //size of each vm's portion
		int[] displ = new int[vmCount]; // start index of each vm's portion
		int offset = 0;
		for(int i=0; i<vmCount;i++){
		            dimPortions[i] = (1000 / vmCount) +
		                            ((i < (1000 % vmCount)) ? 1 : 0);
		            displ[i] = offset;
		            offset += dimPortions[i];
		}
		int numberOfFunctions = 1000;
		
		for(int i = 0; i < smpThreadCount; i++) {
			__thread_pool_smp.submit(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					while(__wait_on_ch) {
						List<String> __recMsgs = azure.peeksFromQueue("ch-"+__id_execution,32);
						for(String msg : __recMsgs) { 
							ch.put(msg);
						}
					}
					return null;
				}
			});
		}
		
		azure.executeFLYonVMCluster(dimPortions,
				displ,
				numberOfFunctions,
				__id_execution);
		
		System.out.print("\n\u27A4 Waiting for FLY execution to complete...");
		while (ch.size() != numberOfFunctions);
		__wait_on_ch = false;
		System.out.println("Done");
		
		estimation();
		
		azure.deleteResourcesAllocated();
		__thread_pool_smp.shutdown();
							
		System.exit(0);
	}
	
	protected static  Object estimation()throws Exception{
		Integer sum = 0;
		
		Integer crt = 0;
		
		
		for(int i=0;i<1000;i++){
			
			{
				
				sum += Integer.parseInt(ch.take().toString());
				
				crt += 1;
			}
		}
		
		System.out.println("pi estimation: " + (sum * 4.0) / crt);
		return null;
		}
	

    
}
