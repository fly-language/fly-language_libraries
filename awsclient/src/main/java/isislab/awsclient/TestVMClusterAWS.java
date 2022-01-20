package isislab.awsclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import com.amazonaws.auth.BasicAWSCredentials;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static AWSClient aws = null;
	
	static long  __id_execution =  System.currentTimeMillis();
	
	static Integer vmCount = 2;	
	static Integer funcCount = 4;
	static Integer N = 20;
	static Integer[] vector = new Integer[N];	


	public static void main(String[] args) throws Exception{
		
		String region = "eu-west-2";
		String instance_type = "t2.micro";
		int vCPUsCount_12 = 1;
		int vmCount_1642607371668 = 2;
		
		Integer [] arrayOfIntegers = {1,2,3,4,5,6,7,8,9};

		try {
			/*
			String myConst = "[{\"name\":\"vector\",\"type\":\"Integer\",\"value\":[8, 2, 0, 6, 9, 9, 8, 0, 9, 8, 0, 1, 1, 8, 4, 7, 9, 0, 1, 9]}]";
			JSONArray jsonArray2 = new JSONArray(myConst);
			
			JSONArray JSONArrayValues = new JSONObject(jsonArray2.get(0).toString()).getJSONArray("value");
			System.out.println(JSONArrayValues.getInt(0));
			*/
			
			aws = new AWSClient(creds,region, "termination_queue_name");
			
			//input array example splitting
			int splitCount_12 = vCPUsCount_12 * vmCount_1642607371668;
			int vmCountToUse_12 = vmCount_1642607371668;
			ArrayList<StringBuilder> __temp_arrayOfIntegers_12 = new ArrayList<StringBuilder>();
			ArrayList<String> portionInputs_12 = new ArrayList<String>();
			int __arr_length_12 = arrayOfIntegers.length;
			
			if ( __arr_length_12 < splitCount_12) splitCount_12 = __arr_length_12;
			if ( splitCount_12 < vmCountToUse_12) vmCountToUse_12 = splitCount_12;				
			
			int[] dimPortions_12 = new int[splitCount_12]; 
			int[] displ_12 = new int[splitCount_12]; 
			int offset_12 = 0;
			
			for(int __i=0;__i<splitCount_12;__i++){
				dimPortions_12[__i] = (__arr_length_12 / splitCount_12) +
					((__i < (__arr_length_12 % splitCount_12)) ? 1 : 0);
				displ_12[__i] = offset_12;								
				offset_12 += dimPortions_12[__i];
				
				__temp_arrayOfIntegers_12.add(__i,new StringBuilder());
				String myArrayPortionString = Arrays.toString(Arrays.copyOfRange(arrayOfIntegers,displ_12[__i], displ_12[__i]+dimPortions_12[__i] ));
				
				__temp_arrayOfIntegers_12.get(__i).append("{\"portionLength\":"+dimPortions_12[__i]+",\"portionIndex\":"+__i+",\"portionDisplacement\":"+displ_12[__i]+",\"portionValues\":\""+myArrayPortionString+"\"}");							
				portionInputs_12.add(__generateString(__temp_arrayOfIntegers_12.get(__i).toString(),12));
				
				System.out.println(__generateString(__temp_arrayOfIntegers_12.get(__i).toString(),12));
			}
			int numberOfFunctions_12 = splitCount_12;
			int notUsedVMs_12 = vmCount_1642607371668 - vmCountToUse_12;
			ArrayList<String> constVariables_12 = new ArrayList<String>();
			
			constVariables_12.add("{\"name\":\"vmCount\",\"type\":\"Integer\",\"value\":"+vmCount+"}");
			constVariables_12.add("{\"name\":\"funcCount\",\"type\":\"Integer\",\"value\":"+funcCount+"}");
			
			Integer min = 0;
			
			Integer max = 10;
			for(int i=0;i<N;i++){
				
				{
					Random r = new Random();
					
					Integer x = r.nextInt(max - min) + min;
					
					
					vector[i] = x;
				}
			}
			
			constVariables_12.add("{\"name\":\"vector\",\"type\":\"Integer\",\"value\":\""+Arrays.deepToString(vector)+"\"}");

			
			System.out.println(constVariables_12.get(2));
			
			
			/*aws.testMethod(portionInputs_12,
					constVariables_12,
					numberOfFunctions_12,
					__id_execution);*/
			
			aws.cleanResources();

			
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
