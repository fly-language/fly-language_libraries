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
	static boolean __wait_on_termination_partialArray_0 = true;

	static 		long  __id_execution =  System.currentTimeMillis();
	//static 		long  __id_execution = 1641659868015L;
	static AzureClient azure;
	
	public static void main(String[] args) throws Exception {

		String clientId = "";
		String tenantId = "";
		String secret = "";
		String subscriptionId = "";
		
		String vmTypeSize_1641303728741 = "Standard_B1s"; //Standard_B1s, Standard_B2s as an alternative, be careful 4 vCPUs are allowed as max with Student Subscription
		String purchasingOption_1641303728741 = "on-demand";
		int vmCount_1641303728741 = 2;
		boolean persistent_1641303728741 = true;
	
		String __termination_partialArray_0 = "termination-partialArray-"+__id_execution;
		azure = new AzureClient(clientId,
				tenantId,
				secret,
				subscriptionId,
				__id_execution+"",
				"West Europe",
				__termination_partialArray_0);
		
		azure.VMClusterInit();
		
		//System.out.println(azure.checkForExecutionErrors());
		//System.out.println(azure.checkBuildingStatus());
		__wait_on_ch=false;
		__wait_on_termination_partialArray_0=true;
		azure.createQueue("termination-partialArray-"+__id_execution);
		final LinkedTransferQueue<String> __termination_partialArray_ch_0  = new LinkedTransferQueue<String>();
		__thread_pool_smp.submit(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				while(__wait_on_termination_partialArray_0) {
					List<String> __recMsgs = azure.peeksFromQueue("termination-partialArray-"+__id_execution,10);
					for(String msg : __recMsgs) { 
						__termination_partialArray_ch_0.put(msg);
					}
				}
				return null;
			}
		});
						
		__wait_on_ch=true;
		azure.createQueue("ch-"+__id_execution);
		//for(int __i=0;__i< (Integer)__fly_environment.get("smp").get("nthread");__i++){ 
			__thread_pool_smp.submit(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					while(__wait_on_ch) {
						List<String> __recMsgs = azure.peeksFromQueue("ch-"+__id_execution,1);
						for(String msg : __recMsgs) { 
							ch.put(msg);
						}
					}
					return null;
				}
			});
		//}
		
		int vCPUsCount_0 = azure.getVCPUsCount(vmTypeSize_1641303728741);
		
		azure.zipAndUploadCurrentProject();
				
		int vmsCreatedCount_0 = azure.launchVMCluster(vmTypeSize_1641303728741, purchasingOption_1641303728741, persistent_1641303728741, vmCount_1641303728741);
		
		if ( vmsCreatedCount_0 != 0) {
			System.out.print("\n\u27A4 Waiting for virtual machines boot script to complete...");
			while ( __termination_partialArray_ch_0.size() != vmsCreatedCount_0);
			System.out.println("Done");
		}
		if(vmsCreatedCount_0 != vmCount_1641303728741){
			if ( vmsCreatedCount_0 > 0) azure.downloadFLYProjectonVMCluster();
			
			System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
			while (__termination_partialArray_ch_0.size() != (vmCount_1641303728741+vmsCreatedCount_0));
		}
		System.out.println("Done");
		
		String mainClass_12 = "TestVMClusterAZURE";
		azure.buildFLYProjectOnVMCluster(mainClass_12);
		
		System.out.print("\n\u27A4 Waiting for building project on VM CLuster to complete...");
		if(vmsCreatedCount_0 != vmCount_1641303728741){
			while ( __termination_partialArray_ch_0.size() != ( (vmCount_1641303728741*2)+vmsCreatedCount_0));
		} else {
			while (__termination_partialArray_ch_0.size() != (vmCount_1641303728741*2));
		}
		System.out.println("Done");
		
		//Check for building errors
		String err_build_12 = azure.checkBuildingStatus();
		if (err_build_12 != null) {
			//Print the error within each VM
			System.out.println("The building failed with the following errors in each VM:");
			System.out.println(err_build_12);
			return;
		}				
		
		//Splitting
		
		//Execution

		//Termination
		System.exit(0);
		
	}
}
