package isislab.awsclient;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

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
		String instance_type = "t2.micro";
		int vCPUsCount_33 = 1;
		int vmCount_1643281687557 = 4;
		boolean persistence = true;
		/*
		Integer[][] matrix = new Integer[M][N];
		
		Integer min = 0;
		
		Integer max = 100;
		
		
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
		
		
		for(int i=0;i<N;i++){
			
			{
				Random r = new Random();
				
				Integer x = r.nextInt(max - min) + min;
				
				
				vector[i] = x;
			}
		}*/

		try {
			/*
			String myConst = "[{\"name\":\"vector\",\"type\":\"Integer\",\"value\":[8, 2, 0, 6, 9, 9, 8, 0, 9, 8, 0, 1, 1, 8, 4, 7, 9, 0, 1, 9]}]";
			JSONArray jsonArray2 = new JSONArray(myConst);
			
			JSONArray JSONArrayValues = new JSONObject(jsonArray2.get(0).toString()).getJSONArray("value");
			System.out.println(JSONArrayValues.getInt(0));
			*/
			
			aws = new AWSClient(creds,region);
			aws.setupS3Bucket("flybucketvmcluster");
			
			File f1 = aws.downloadS3ObjectToFile("mySplitsi-02879fc2998be7349.txt");
			File f2 = aws.downloadS3ObjectToFile("constValues.txt");
			
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
			
			int x = 0;
			JSONObject constJsonObject = null;
			JSONArray constJsonArray = null;
			constJsonObject = new JSONObject(myConsts.get(x));
			x++;
			
			vmCount = (Integer) constJsonObject.get("value");
			constJsonObject = new JSONObject(myConsts.get(x));
			x++;
			
			funcCount = (Integer) constJsonObject.get("value");
			constJsonObject = new JSONObject(myConsts.get(x));
			x++;
			
			M = (Integer) constJsonObject.get("value");
			constJsonObject = new JSONObject(myConsts.get(x));
			x++;
			
			N = (Integer) constJsonObject.get("value");
			constJsonObject = new JSONObject(myConsts.get(x));
			x++;
			
			constJsonArray = new JSONArray(constJsonObject.get("value").toString());
			for (int j=0; j< vector.length; j++){
				vector[j] = constJsonArray.getInt(j);
			}
			
			System.out.println(Arrays.deepToString(vector));
			
			
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
			ArrayList<String> constVariables_33 = new ArrayList<String>();
			
			constVariables_33.add("{\"name\":\"vmCount\",\"type\":\"Integer\",\"value\":"+vmCount+"}");
			
			constVariables_33.add("{\"name\":\"funcCount\",\"type\":\"Integer\",\"value\":"+funcCount+"}");
						
			constVariables_33.add("{\"name\":\"M\",\"type\":\"Integer\",\"value\":"+M+"}");
			
			constVariables_33.add("{\"name\":\"N\",\"type\":\"Integer\",\"value\":"+N+"}");
			
			constVariables_33.add("{\"name\":\"vector\",\"type\":\"Array_Integer\",\"value\":\""+Arrays.deepToString(vector)+"\"}");
						
			
			aws.executeFLYonVMCluster(portionInputs_33,
					constVariables_33,
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
