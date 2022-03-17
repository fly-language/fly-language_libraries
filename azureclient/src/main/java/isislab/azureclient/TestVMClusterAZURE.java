package isislab.azureclient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
	static 		long  __id_execution =  1647526965609L;
	static Integer M = 100;	
	static Integer N = 100;	
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
		
		Integer[][] matrix = new Integer[M][N];
		
		Integer min = 0;
		
		Integer max = 10;
		
		
		for(int i=0;i<M;i++){
			
			{
				
				for(int j=0;j<N;j++){
					
					{
						Random r = new Random();
						
						Integer x = r.nextInt(max - min) + min;
						
						
						matrix[i][j] =  x;
					}
				}
			}
		}
	
		final String __termination_pi_0 = "termination-pi-"+__id_execution;
		azure = new AzureClient(clientId,
				tenantId,
				secret,
				subscriptionId,
				__id_execution+"",
				"West Europe",
				__termination_pi_0);
		
		azure.VMClusterInit();
	
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
		
		/*
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
		//}*/
		
		int vCPUsCount_5 = azure.getVCPUsCount(vmTypeSize_1641303728741);
		
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
		
		String mainClass_12 = "MatrixVectorMultiplication_matrixVectorMultiplication";
		azure.buildFLYProjectOnVMCluster(mainClass_12);
		
		System.out.print("\n\u27A4 Waiting for building project on VM CLuster to complete...");
		if(vmsCreatedCount_0 != vmCount_1641303728741){
			while ( __termination_pi_ch_0.size() != ( (vmCount_1641303728741*2)+vmsCreatedCount_0));
		} else {
			while (__termination_pi_ch_0.size() != (vmCount_1641303728741*2));
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
		int splitCount_5 = vCPUsCount_5 * vmCount_1641303728741;
		int vmCountToUse_5 = vmCount_1641303728741;
		ArrayList<StringBuilder> __temp_matrix_5 = new ArrayList<StringBuilder>();
		ArrayList<String> portionInputs_5 = new ArrayList<String>();
		
		int __rows_5 = matrix.length;
		int __cols_5 = matrix[0].length;
		
		int __current_row_matrix_5 = 0;
															
		if ( __rows_5 < splitCount_5) splitCount_5 = __rows_5;
		if ( splitCount_5 < vmCountToUse_5) vmCountToUse_5 = splitCount_5;
									
		int[] dimPortions_5 = new int[splitCount_5]; 
		int[] displ_5 = new int[splitCount_5]; 
		int offset_5 = 0;
									
		for(int __i=0;__i<splitCount_5;__i++){
			dimPortions_5[__i] = (__rows_5 / splitCount_5) + ((__i < (__rows_5 % splitCount_5)) ? 1 : 0);
			displ_5[__i] = offset_5;								
			offset_5 += dimPortions_5[__i];
										
			__temp_matrix_5.add(__i,new StringBuilder());
			__temp_matrix_5.get(__i).append("{\"portionRows\":"+dimPortions_5[__i]+",\"portionCols\":"+__cols_5+",\"portionIndex\":"+__i+",\"portionDisplacement\":"+displ_5[__i]+",\"portionValues\":[");							
				
			for(int __j=__current_row_matrix_5; __j<__current_row_matrix_5+dimPortions_5[__i];__j++){
				for(int __z = 0; __z<matrix[__j].length;__z++){
					__temp_matrix_5.get(__i).append("{\"x\":"+__j+",\"y\":"+__z+",\"value\":"+matrix[__j][__z]+"},");
				}
				if(__j == __current_row_matrix_5 + dimPortions_5[__i]-1) {
					__temp_matrix_5.get(__i).deleteCharAt(__temp_matrix_5.get(__i).length()-1);
					__temp_matrix_5.get(__i).append("]}");
				}
			}
			__current_row_matrix_5 +=dimPortions_5[__i];
			portionInputs_5.add(__generateString(__temp_matrix_5.get(__i).toString(),5));
		}
		int numberOfFunctions_5 = splitCount_5;
		int notUsedVMs_5 = vmCount_1641303728741 - vmCountToUse_5;
		azure.executeFLYonVMCluster(portionInputs_5,
										numberOfFunctions_5,
										__id_execution);

		System.out.print("\n\u27A4 Waiting for FLY execution to complete...");
		if(vmsCreatedCount_0 != vmCount_1641303728741){
		while (__termination_pi_ch_0.size() != ( (vmCount_1641303728741*3)+vmsCreatedCount_0-notUsedVMs_5));
		} else {
		while (__termination_pi_ch_0.size() != (vmCount_1641303728741*3-notUsedVMs_5 ));
		}
		__wait_on_termination_pi_0=false;
		System.out.println("Done");
		
		//Check for execution errors
		String err_exec_0 = azure.checkForExecutionErrors();
		if (err_exec_0 != null) {
			//Print the error within each VM
			System.out.println("The execution failed with the following errors in each VM:");
			System.out.println(err_exec_0);
		}else {
			//No execution errors
			//Manage the callback
			//aggregateResults();
		}
		
		//Clear termination queue for eventual next iteration
		__termination_pi_ch_0.clear();
		
		//__wait_on_ch=false;
		
		//azure.deleteResourcesAllocated();
		
		__thread_pool_local.shutdown();

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
