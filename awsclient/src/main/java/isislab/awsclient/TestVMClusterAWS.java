package isislab.awsclient;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.auth.BasicAWSCredentials;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static AWSClient aws = null;
	
	static long  __id_execution =  System.currentTimeMillis();
	
	static Integer vmCount = 2;	
	static Integer funcCount = 4;
	static Integer M = 10;	
	static Integer N = 10;	
	static Integer[] vector = new Integer[N];
	
	static ExecutorService __thread_pool_smp = Executors.newFixedThreadPool(4);
/*
	static AmazonSQS __sqs_aws = AmazonSQSClientBuilder.standard()
			.withRegion("eu-west-2")							 
			.withCredentials(new AWSStaticCredentialsProvider(creds))
			.build();*/
	
	static boolean __wait_on_termination_matrixVectorMultiplication_0 = true;


	public static void main(String[] args) throws Exception{
		
		String region = "eu-west-2";
		String vmTypeSize_1643452623920 = "c4.large";
		int vmCount_1643452623920 = 4;
		boolean persistent_1643452623920 = true;
		String purchasingOption_1643452623920 = "spot";
		int vCPUsCount_33 = 2;

		try {
			
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
			System.out.println((Arrays.deepToString(matrix)));
			
			//input array example splitting
			int splitCount_33 = vCPUsCount_33 * vmCount_1643452623920;
			int vmCountToUse_33 = vmCount_1643452623920;
			
			int __rows_33 = matrix.length;
			int __cols_33 = matrix[0].length;
			
			int __current_row_matrix_33 = 0;
																
			if ( __rows_33 < splitCount_33) splitCount_33 = __rows_33;
			if ( splitCount_33 < vmCountToUse_33) vmCountToUse_33 = splitCount_33;
										
			int dimPortion_33 = 0;
			int displ_33 = 0;
			int offset_33 = 0;
										
			for(int __i=0;__i<splitCount_33;__i++){
				dimPortion_33 = (__rows_33 / splitCount_33) + ((__i < (__rows_33 % splitCount_33)) ? 1 : 0);
				displ_33 = offset_33;								
				offset_33 += dimPortion_33;
											
				StringBuilder __temp_matrix_33 = new StringBuilder();
				__temp_matrix_33.append("{\"portionRows\":"+dimPortion_33+",\"portionCols\":"+__cols_33+",\"portionIndex\":"+__i+",\"portionDisplacement\":"+displ_33+",\"portionValues\":[");							
				
				for(int __j=__current_row_matrix_33; __j<__current_row_matrix_33+dimPortion_33;__j++){
					for(int __z = 0; __z<matrix[__j].length;__z++){
						__temp_matrix_33.append("{\"x\":"+__j+",\"y\":"+__z+",\"value\":"+matrix[__j][__z]+"},");
					}
					if(__j == __current_row_matrix_33 + dimPortion_33-1) {
						__temp_matrix_33.deleteCharAt(__temp_matrix_33.length()-1);
						__temp_matrix_33.append("]}");
					}
				}
				__current_row_matrix_33 +=dimPortion_33;
				//portionInputs_33.add(__generateString(__temp_matrix_33.get(__i).toString(),33));
				FileOutputStream output = new FileOutputStream("test_"+__i);
				try {
				  Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
				  try {
				    writer.write(__temp_matrix_33.toString());
				  } finally {
				    writer.close();
				  }
				 } finally {
				   output.close();
				 }
				
			}
			int numberOfFunctions_33 = splitCount_33;
			int notUsedVMs_33 = vmCount_1643452623920 - vmCountToUse_33;
			
			/*GZIPInputStream is =  new GZIPInputStream(new FileInputStream("test_0"));
			InputStreamReader reader = new InputStreamReader(is);
			BufferedReader in = new BufferedReader(reader);

			ArrayList<String> __mySplits = new ArrayList<>();
			int __mySplitsCount = 0;
			String readed;
			while ((readed = in.readLine()) != null) {
				__mySplits.add(readed);
			}
			System.out.println(__mySplits);
			System.out.println("\n" + __mySplits);
			is.close();
			reader.close();
			in.close();*/

			
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
