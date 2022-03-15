package isislab.azureclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

public class TestVMClusterAZURE {
	
	static ExecutorService __thread_pool_local = Executors.newFixedThreadPool(4);
	static LinkedTransferQueue<Object> ch = new LinkedTransferQueue<Object>();
	static Boolean __wait_on_ch = true;
	static boolean __wait_on_termination_pi_0 = true;

	//static 		long  __id_execution =  System.currentTimeMillis();
	static long __id_execution = 1647359040218L;
	static AzureClient azure;
	
	public static void main(String[] args) throws Exception {

		String clientId = "6501f541-fad9-4bc9-91a5-1829d7f532bf";
		String tenantId = "c30767db-3dda-4dd4-8a4d-097d22cb99d3";
		String secret = "v2i1fR0v3agHx7Hh7Gf-xQ-1_hBc_TErp6";
		String subscriptionId = "435d02f4-3bb6-4c41-bee0-c330507d849c";
		
		String vmTypeSize_1641303728741 = "Standard_B1s"; //Standard_B1s, Standard_B2s as an alternative, be careful 4 vCPUs are allowed as max with Student Subscription
		String purchasingOption_1641303728741 = "on-demand";
		int vmCount_1641303728741 = 2;
		boolean persistent_1641303728741 = true;
	
		final String __termination_pi_0 = "termination-pi-"+__id_execution;
		azure = new AzureClient(clientId,
				tenantId,
				secret,
				subscriptionId,
				__id_execution+"",
				"West Europe",
				__termination_pi_0);
	
		__wait_on_ch=false;
		__wait_on_termination_pi_0=true;
		azure.createQueue("termination-pi-"+__id_execution);
		LinkedTransferQueue<String> __termination_pi_ch_0  = new LinkedTransferQueue<String>();
		__thread_pool_local.submit(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				while(__wait_on_termination_pi_0) {
					List<String> __recMsgs = azure.peeksFromQueue("termination-pi-"+__id_execution,10);
					for(String msg : __recMsgs) { 
						__termination_pi_ch_0.put(msg);
					}
				}
				return null;
			}
		});
						
		__wait_on_ch=true;
		azure.createQueue("ch-"+__id_execution);
		//for(int __i=0;__i< (Integer)__fly_environment.get("local").get("nthread");__i++){ 
			__thread_pool_local.submit(new Callable<Object>() {
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
			while ( __termination_pi_ch_0.size() != vmsCreatedCount_0);
			System.out.println("Done");
		}
		if(vmsCreatedCount_0 != vmCount_1641303728741){
			if ( vmsCreatedCount_0 > 0) azure.downloadFLYProjectonVMCluster();
			
			System.out.print("\n\u27A4 Waiting for download project on VM CLuster to complete...");
			while (__termination_pi_ch_0.size() != (vmCount_1641303728741+vmsCreatedCount_0));
		}
		System.out.println("Done");
		
		int vmCount_7 = vmCount_1641303728741;
		int numberOfFunctions_7 = 100 - 0;
		
		ArrayList<StringBuilder> __temp_7 = new ArrayList<StringBuilder>();
		ArrayList<String> portionInputs_7 = new ArrayList<String>();
		
		if ( vmCount_7 > numberOfFunctions_7) vmCount_7 = numberOfFunctions_7;
																	
		int[] dimPortions_7 = new int[vmCount_7]; 
		int[] displ_7 = new int[vmCount_7]; 
		int offset_7 = 0;
										
		for(int __i=0; __i<vmCount_7;__i++){
			dimPortions_7[__i] = (numberOfFunctions_7 / vmCount_7) +
				((__i < (numberOfFunctions_7 % vmCount_7)) ? 1 : 0);
			displ_7[__i] = offset_7;
			offset_7 += dimPortions_7[__i];
			
			__temp_7.add(__i,new StringBuilder());
			__temp_7.get(__i).append("{\"portionRangeLength\":"+dimPortions_7[__i]+",\"portionRangeIndex\":"+__i+"}");							
			portionInputs_7.add(__generateString(__temp_7.get(__i).toString(),7));
		}
		numberOfFunctions_7 = vmCount_7;
		int notUsedVMs_7 = vmCount_1641303728741 - vmCount_7;
				
		/*String mainClass_12 = "TestVMClusterAZURE";
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
		}*/
		
		
		//Splitting
		
		//Execution

		//Termination
		System.exit(0);
		
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
