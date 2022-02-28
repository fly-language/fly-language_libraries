package isislab.awsclient;

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class OnDemandInstancesHandler {
	
	private AmazonEC2 ec2;
    
	protected OnDemandInstancesHandler (AmazonEC2 ec2) {
        this.ec2 = ec2;
    }
    
	protected List<Instance> createOnDemandInstances(String amiId, String securityGroupName, String keyPairName, String instance_type, 
			String bucketName, IamInstanceProfileSpecification iamInstanceProfile, int vmCount, String queueUrl) {
		
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		runInstancesRequest.withImageId(amiId)
				   .withInstanceType(InstanceType.fromValue(instance_type))
				   .withMinCount(vmCount)
				   .withMaxCount(vmCount)
				   .withKeyName(keyPairName)
				   .withSecurityGroups(securityGroupName)
				   .withUserData(EC2Handler.getUserData(bucketName, queueUrl))
				   .withIamInstanceProfile(iamInstanceProfile);
		
		int i=0;
		int maxRetries = 10000;
		while(i<maxRetries) {
			try {
				RunInstancesResult run_response = ec2.runInstances(runInstancesRequest);
				
				System.out.println("\n\u27A4 Creating and starting VM Cluster (on-demand) on AWS");
				
				return run_response.getReservation().getInstances();
			}catch(AmazonEC2Exception e) {
				i++;
			}
		}
		System.out.println("Number of tries exceeded");
		return null;
	}

}
