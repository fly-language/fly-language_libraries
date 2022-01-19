package isislab.awsclient;

import com.amazonaws.auth.BasicAWSCredentials;

public class TestVMClusterAWS {
	
	static BasicAWSCredentials creds = new BasicAWSCredentials("", "");
	static AWSClient aws = null;

	public static void main(String[] args) throws Exception{
		
		String region = "eu-west-2";
		String instance_type = "t3.micro";
		
		try {			
			//Channels handling
			aws = new AWSClient(creds,region, "termination_queue_name");
			
			//int vCPUsCount_8 = aws.getVCPUsCount(instance_type);
			//System.out.println(vCPUsCount_8);
			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			System.exit(0);
		}	
	}

}
