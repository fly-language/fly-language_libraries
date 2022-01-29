package isislab.awsclient;

import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupEgressRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CancelSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.waiters.AmazonEC2Waiters;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.AddRoleToInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.AddRoleToInstanceProfileResult;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreateInstanceProfileRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;


public class EC2Handler {
	
	private AmazonEC2 ec2;
	private AmazonIdentityManagement iamClient;
	private OnDemandInstancesHandler onDemandHandler;
	private SpotInstancesHandler spotHandler;
	private RunCommandHandler runCommandHandler;
	private S3Handler s3Handler;
	private boolean persistent;

	private List<Instance> virtualMachines;
    
	protected EC2Handler (AmazonEC2 ec2, AmazonIdentityManagement iamClient, RunCommandHandler runCommandHandler, S3Handler s3Handler) {
    	this.ec2 = ec2;
    	this.iamClient = iamClient;
    	this.runCommandHandler = runCommandHandler;
    	this.s3Handler = s3Handler;
        onDemandHandler = new OnDemandInstancesHandler(ec2);
		spotHandler = new SpotInstancesHandler(ec2);
		this.virtualMachines = new ArrayList<Instance>();
    }
    
	protected int createVirtualMachinesCluster(String instance_type, String bucketName, String purchasingOption, boolean persistent, 
			int vmCount, String queueUrl) throws InterruptedException, ExecutionException {
    	
    	if (!checkCorrectInstanceType(instance_type)) System.exit(1);  //Instance_type not correct
    	
		String amiId = "ami-005383956f2e5fb96";
    	String securityGroupName = "flySecurityGroup";
    	String keyPairName = "flyKeyPair";
    	String instanceProfileName = "roleForEc2";
        	
	    //Check if a new VM Cluster has to be created or we could use an existent one
    	this.persistent = persistent;
    	boolean moreVMneeded = false;
    	String persistenceVMCluster = checkVMClusterPersistence(vmCount, persistent, amiId, instance_type, purchasingOption);
    	
    	if ( persistenceVMCluster.equals("terminate")) {
    		//Terminate the existent VM cluster because not appropriate
    		System.out.println("\n\u27A4 The existent VM Cluster hasn't the characteristics requested, so it will be terminated, and then will be created a new one.");
    		deleteResourcesAllocated(true, bucketName);
    	} else if (persistenceVMCluster.equals("reuse")) {
    		//An existent VM Cluster with requested characteristics exists so use it
    		System.out.println("\n\u27A4 Using existent VM Cluster");
    		//Download necessary files for the execution on existent cluster
    		this.runCommandHandler.setVMs(this.virtualMachines);
    		this.runCommandHandler.downloadExecutionFileOnVMCluster(bucketName, queueUrl);
    		return 0;
    	} else if( !persistenceVMCluster.equals("create") ) {
    		//More VMs are needed for the cluster
    		vmCount = Integer.parseInt(persistenceVMCluster);
    		moreVMneeded = true;
    	}

		createSecurityGroupIfNotExists(securityGroupName);
		createKeyPairIfNotExists(keyPairName);
		createInstanceProfileIfNotExists(instanceProfileName);
		
        if (moreVMneeded) System.out.print("\n\u27A4 The existent cluster has not enough VMs...Creating additional "+vmCount+" VMs for the Cluster...");
        else System.out.print("\n\u27A4 Creating "+vmCount+" VMs for the Cluster...");
		
		IamInstanceProfileSpecification iamInstanceProfile = new IamInstanceProfileSpecification().withName(instanceProfileName);
		
		//Check purchasing option: on-demand or spot
		if( purchasingOption.equals("on-demand")) {
			
			this.virtualMachines.addAll(onDemandHandler.createOnDemandInstances(amiId, securityGroupName, keyPairName, 
					instance_type, bucketName, iamInstanceProfile, vmCount, queueUrl));
			
		}else if( purchasingOption.equals("spot")) {
			
			this.virtualMachines.addAll(spotHandler.createSpotInstances(amiId, securityGroupName, keyPairName, 
					instance_type, bucketName, iamInstanceProfile, vmCount, queueUrl));
		}
		
		this.runCommandHandler.setVMs(this.virtualMachines);

		nameInstances();
		
		//Wait until the instances are effectively running and status check is OK
		waitUntilClusterIsReady();
		
        System.out.println("Done");

        return vmCount;
	}
    
    private void nameInstances() {
		//Add a name to each instance of the cluster
    	String name = "flyVM";
    	Tag tag;
    	CreateTagsRequest tag_request;
		
    	for (int i=0; i < this.virtualMachines.size(); i++) {
    		tag = new Tag()
    				.withKey("Name")
    				.withValue(name+UUID.randomUUID().toString());
    		
    		tag_request = new CreateTagsRequest()
    			    .withResources(this.virtualMachines.get(i).getInstanceId())
    			    .withTags(tag);
    		
    		ec2.createTags(tag_request);
    	}
    }
        
    
    //Check if VM Instance type specified is an existent one
    private boolean checkCorrectInstanceType(String instance_type) {
    	if (instance_type == null || "".equals(instance_type)) {
            System.out.println("\n**ERROR: Instance_type cannot be null or empty!");
            return false;
        }

        for (InstanceType enumEntry : InstanceType.values()) {
            if (enumEntry.toString().equals(instance_type)) {
                return true;
            }
        }

        System.out.println("\n**ERROR: Instance_type chosen: " + instance_type + " not existent or not available in the region selected.");
        return false;
    }
 
    private void createSecurityGroupIfNotExists(String securityGroupName) {

		boolean exists = false;

		DescribeSecurityGroupsResult describeSecurityGroupsResult = ec2.describeSecurityGroups();
		List<SecurityGroup> securityGroups = describeSecurityGroupsResult.getSecurityGroups();
		
		for (SecurityGroup securityGroup : securityGroups) {
		    	//check if your security group exists
		    	if(securityGroup.getGroupName().equals(securityGroupName)) exists = true;
		}
	
		if(!exists) {
			//The security group with the given name does not exist so create it
			
			CreateSecurityGroupRequest createSecurityGroupRequest = new CreateSecurityGroupRequest();
			createSecurityGroupRequest.withGroupName(securityGroupName).withDescription("fly security group");
			String groupId = ec2.createSecurityGroup(createSecurityGroupRequest).getGroupId();
			
			//Set Permissions
			IpPermission ipPermission = new IpPermission();
			IpRange ipRange1 = new IpRange().withCidrIp("0.0.0.0/0");
			
			// SSH Permissions
			ipPermission.withIpv4Ranges(Arrays.asList(new IpRange[] {ipRange1}))
						.withIpProtocol("tcp")
						.withFromPort(22)
						.withToPort(22);
	       	 	
	       	 	AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =	new AuthorizeSecurityGroupIngressRequest();
	       	 	authorizeSecurityGroupIngressRequest.withGroupName(securityGroupName).withIpPermissions(ipPermission);
	       	 	ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
	       	 	
	       	 	AuthorizeSecurityGroupEgressRequest authorizeSecurityGroupEgressRequest = new AuthorizeSecurityGroupEgressRequest();
	       	 	authorizeSecurityGroupEgressRequest.withGroupId(groupId).withIpPermissions(ipPermission);
	       	 	ec2.authorizeSecurityGroupEgress(authorizeSecurityGroupEgressRequest);

	       	 	// HTTP Permissions
	       	 	ipPermission = new IpPermission();
	     	
				ipPermission.withIpv4Ranges(Arrays.asList(new IpRange[] {ipRange1}))
								.withIpProtocol("tcp")
								.withFromPort(80)
								.withToPort(80);
	    	 	
	       	 	authorizeSecurityGroupIngressRequest =	new AuthorizeSecurityGroupIngressRequest();
	       	 	authorizeSecurityGroupIngressRequest.withGroupName(securityGroupName).withIpPermissions(ipPermission);
	       	 	ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
	       	 	
	       	 	authorizeSecurityGroupEgressRequest = new AuthorizeSecurityGroupEgressRequest();
	       	 	authorizeSecurityGroupEgressRequest.withGroupId(groupId).withIpPermissions(ipPermission);
	       	 	ec2.authorizeSecurityGroupEgress(authorizeSecurityGroupEgressRequest);
		}
	}
		
	private void createKeyPairIfNotExists(String keyPairName) {
			
		boolean exists = false;
		
		DescribeKeyPairsResult describeKeyPairsResult = ec2.describeKeyPairs();
		List<KeyPairInfo> keyPairsInfo = describeKeyPairsResult.getKeyPairs();
			
		for (KeyPairInfo keyPair : keyPairsInfo) {
		    	//check if your key pair exists
		    	if(keyPair.getKeyName().equals(keyPairName)) exists = true;
		}
		
		if(!exists) {
	       	 	//The key pair with the given name does not exist, so create it
	       	 	CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest().withKeyName(keyPairName);
	       	 	ec2.createKeyPair(createKeyPairRequest);
		}
	}
	
	private void createInstanceProfileIfNotExists(String instanceProfileName) {
			//Check if the instance profile exists
			for (InstanceProfile p : iamClient.listInstanceProfiles().getInstanceProfiles()) if(p.getInstanceProfileName().equals(instanceProfileName)) return; //Instance profile already existent
		
	    	//Instance profile has to be created, so check first if the role exists
			Boolean roleExists = false;
			String roleName = instanceProfileName;
	    	for (Role r : iamClient.listRoles().getRoles()) if(r.getRoleName().equals(roleName)) roleExists = true; //Role already existent

	    	if(!roleExists) {
	    		//The role does not exist, so create it
	    		try {
			    	//The specified role does not exist
				    String assumeRolefileLocation = "assumeRole.json";
		    		FileReader reader = new FileReader(assumeRolefileLocation);
		            JSONParser jsonParser = new JSONParser();
		            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
		
		            CreateRoleRequest roleRequest = new CreateRoleRequest()
		            		.withRoleName(roleName)
		            		.withAssumeRolePolicyDocument(jsonObject.toJSONString());
		            
		            iamClient.createRole(roleRequest);
		            
				    String policyfileLocation = "roleForEc2Policy.json";
		    		reader = new FileReader(policyfileLocation);
		            jsonParser = new JSONParser();
		            jsonObject = (JSONObject) jsonParser.parse(reader);

		            CreatePolicyRequest policyRequest = new CreatePolicyRequest()
		            		.withPolicyDocument(jsonObject.toJSONString())
		            		.withPolicyName("ec2RolePolicy");
	   
		            CreatePolicyResult res = iamClient.createPolicy(policyRequest);
		            String policyARN = res.getPolicy().getArn();
		            
		            iamClient.attachRolePolicy(new AttachRolePolicyRequest().withPolicyArn(policyARN).withRoleName(roleName));
		
		        } catch (Exception e) {
		            e.printStackTrace();
		        }
	    	}
	    	
	    	//Create instance profile
	    	iamClient.createInstanceProfile(new CreateInstanceProfileRequest().withInstanceProfileName(instanceProfileName));
	    	//Now in both cases, the role is existent, so add the role to the instance profile created
	    	AddRoleToInstanceProfileResult r = iamClient.addRoleToInstanceProfile(new AddRoleToInstanceProfileRequest().withInstanceProfileName(instanceProfileName).withRoleName(roleName));
	}

	
	protected void deleteResourcesAllocated(boolean terminateClusterNotMatching, String bucketName) {
		
		if(this.persistent && !terminateClusterNotMatching) {
			//Don't terminate the cluster, it could be reused in the next execution
		    System.out.println("\n\u27A4 Resource cleaning");

    		this.s3Handler.deleteBucketWithItsContent(bucketName, true);
		    
    		System.out.print("   \u2022 Deleting document commands...");
    		this.runCommandHandler.deleteFLYdocumentsCommand();
		    System.out.println("Done");

    		System.out.println("\n\u27A4 The VM Cluster is still running and it is ready for the next execution.");
			return;
		}
		
		if ( (!this.persistent) || terminateClusterNotMatching) {
			//Terminate the instances permanently
			List<String> instanceIdsToTerminate = new ArrayList<>();
			
			if (this.virtualMachines.size() == 0) {
				//Take the FLY instances of the cluster to delete				
				DescribeInstancesResult describeInstancesRes = ec2.describeInstances();
				List<Reservation> reservations = describeInstancesRes.getReservations();
					
				for (Reservation reservation : reservations) {
				    for (Instance instance : reservation.getInstances()) {
				    	//check if there are instances with the "flyVMX" name
				    	if(instance.getState().getName().equals("running") && instance.getTags() != null) {
				    		for (Tag tag : instance.getTags()) {
				    			if( tag.getKey().equals("Name") && tag.getValue().contains("flyVM")) this.virtualMachines.add(instance);
				            }
				    	}
				    }
				}
			}
			
			for (Instance instance : this.virtualMachines) instanceIdsToTerminate.add(instance.getInstanceId());
			
			TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
					.withInstanceIds(instanceIdsToTerminate);
			
		    System.out.println("\n\u27A4 Deleting resources used");
		    
		    System.out.print("   \u2022 Deleting virtual machines of the cluster...");

			TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
			List<InstanceStateChange> terminatingInstances = result.getTerminatingInstances();
			
			List<String> terminatingInstanceIds = new ArrayList<>();
			for (int i=0; i< terminatingInstances.size(); i++) {
				terminatingInstanceIds.add(terminatingInstances.get(i).getInstanceId());
			}
			
			//Wait until the instance is effectively terminated
			AmazonEC2Waiters waiter = new AmazonEC2Waiters(ec2);
			DescribeInstancesRequest describeRequest = new DescribeInstancesRequest().withInstanceIds(terminatingInstanceIds);
			WaiterParameters<DescribeInstancesRequest> params = new WaiterParameters<DescribeInstancesRequest>(describeRequest);
	     
			try{
				waiter.instanceTerminated().run(params);
				
			} catch(AmazonServiceException | WaiterTimedOutException | WaiterUnrecoverableException e) {
				System.out.println("   \u2022 An exception is occurred during instance termination.");
				System.exit(1);
			}
		    System.out.println("Done");

			
			//If the terminated instances are spot, terminate also the spot requests associated
			boolean spotRequestsCancel = false;
			for (Instance instance : this.virtualMachines) {
				if ( checkIfInstanceIsSpot(instance)) {
					try {
			            ArrayList<String> spotInstanceRequestIds = new ArrayList<String>();
			            spotInstanceRequestIds.add(instance.getSpotInstanceRequestId());

			            CancelSpotInstanceRequestsRequest cancelRequest = new CancelSpotInstanceRequestsRequest(spotInstanceRequestIds);
			            ec2.cancelSpotInstanceRequests(cancelRequest);
			            spotRequestsCancel = true;
			        } catch (AmazonServiceException e) {
			            // Write out any exceptions that may have occurred.
			            System.out.println("Error cancelling instances");
			            System.out.println("Caught Exception: " + e.getMessage());
			            System.out.println("Reponse Status Code: " + e.getStatusCode());
			            System.out.println("Error Code: " + e.getErrorCode());
			            System.out.println("Request ID: " + e.getRequestId());
			        }
				}
			}
			if (spotRequestsCancel) System.out.println("   \u2022 Spot request associated cancelled.");
			this.virtualMachines.clear();
		}
		
		if( !terminateClusterNotMatching) {
    		System.out.print("   \u2022 Emptying the bucket...");
			this.s3Handler.deleteBucketWithItsContent(bucketName, false);
		    System.out.println("Done");

    		System.out.print("   \u2022 Deleting document commands...");
    		this.runCommandHandler.deleteFLYdocumentsCommand();
		    System.out.println("Done");

		}
		
	}
	
	private void deleteResourcesNotNeeded(List<String> vmsIdNotNeeded) {
		//Terminate useless VMs in the cluster
		System.out.println("\n\u27A4 Deleting resources not appropriate");
 		System.out.print("   \u2022 Terminating VMs not needed and resource related...");
			
		TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
				.withInstanceIds(vmsIdNotNeeded);
		
		TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
		List<InstanceStateChange> terminatingInstances = result.getTerminatingInstances();
		
		List<String> instanceIdsToTerminate = new ArrayList<>();
		for (int i=0; i< terminatingInstances.size(); i++) {
			instanceIdsToTerminate.add(terminatingInstances.get(i).getInstanceId());
		}
		
		//Wait until the instance is effectively terminated
		AmazonEC2Waiters waiter = new AmazonEC2Waiters(ec2);
		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest().withInstanceIds(instanceIdsToTerminate);
		WaiterParameters<DescribeInstancesRequest> params = new WaiterParameters<DescribeInstancesRequest>(describeRequest);
     
		try{
			waiter.instanceTerminated().run(params);
			
		} catch(AmazonServiceException | WaiterTimedOutException | WaiterUnrecoverableException e) {
			System.out.println("   \u2022 An exception is occurred during instance termination.");
			System.exit(1);
		}
	    System.out.println("Done");
	}
	
	private boolean checkInstanceCharacteristics(Instance i, String amiId, String instance_type, String purchasingOption) {
		
		boolean fullCheckDone = false;

		//Check if the requested characteristics match the existent instance
		if( i.getImageId().equals(amiId) &&
			i.getInstanceType().equals(instance_type)) {
				
			//Check purchasing option -> spot or on-demand
			if ( (checkIfInstanceIsSpot(i) && purchasingOption.equals("spot")) ||
				( (!checkIfInstanceIsSpot(i)) && purchasingOption.equals("on-demand"))	) fullCheckDone = true;
		}
		return fullCheckDone;
	}
	
	private boolean checkIfInstanceIsSpot(Instance instance) {

    	//Check if there is the spot request id, if null or blank it is an on-demand instance, otherwise it is spot
    	if(instance.getSpotInstanceRequestId() != null && !instance.getSpotInstanceRequestId().equals("")) return true;
    	else return false;
	}
	
	//Check if there is a cluster of VMs already created with the requested characteristics
	private String checkVMClusterPersistence(int vmCount, boolean persistent, String amiId, String clusterVMInstanceType, String purchasingOption) {
		
		//Check if there are existent VMs to compose the cluster
		List<Instance> vmsAvailable = new ArrayList<Instance>();
		
		DescribeInstancesResult describeInstancesRes = ec2.describeInstances();
		List<Reservation> reservations = describeInstancesRes.getReservations();
			
		for (Reservation reservation : reservations) {
		    for (Instance instance : reservation.getInstances()) {
		    	//check if there are instances with the "flyVMX" name
		    	if(instance.getState().getName().equals("running") && instance.getTags() != null) {
		    		for (Tag tag : instance.getTags()) {
		    			if( tag.getKey().equals("Name") && tag.getValue().contains("flyVM")) vmsAvailable.add(instance);
		            }
		    	}
		    }
		}
		
		if( vmsAvailable.size() > 0) {
			//Check if the available VMs have the characteristics desired
			boolean characteristicsMatching = true;
			for (Instance instance : vmsAvailable) {
				if (! checkInstanceCharacteristics(instance, amiId, clusterVMInstanceType, purchasingOption)) characteristicsMatching = false;
			}
			
			if (characteristicsMatching) {
				//The available VMs have the characteristics desired, so finally check if the number of instances requested is the same
				if (vmsAvailable.size() == vmCount) {
					this.virtualMachines = vmsAvailable;
					this.runCommandHandler.setVMs(vmsAvailable);
					return "reuse";
				}else if (vmsAvailable.size() < vmCount) {
					this.virtualMachines = vmsAvailable;
					this.runCommandHandler.setVMs(vmsAvailable);
					//return the number of VMs additional needed
					return ""+(vmCount - vmsAvailable.size());
				}else {
					//There are more VMs than needed in this execution, so use less VMs
				    System.out.print("\n\u27A4 Deleting resources not needed...");
					int numberOfVmToNotNeeded = vmsAvailable.size() - vmCount;
					List<String> vmsIdNotNeeded = new ArrayList<>(); 
					for (int i = 0; i < numberOfVmToNotNeeded; i++) {
						vmsIdNotNeeded.add(vmsAvailable.get(i).getInstanceId());
						vmsAvailable.remove(i);
					}
					deleteResourcesNotNeeded(vmsIdNotNeeded);
					this.virtualMachines = vmsAvailable;
					this.runCommandHandler.setVMs(vmsAvailable);
				    System.out.println("Done");
					return "reuse";
				}
			}else {
				//Characteristics not matching, so delete the existent VMs and create a new VM cluster
				return "terminate";
			}
		}
		//The are no VMs available
		return "create";
	}
	
	
	//Boot Script for VM to install software dependencies
	protected static final String getUserData(String bucketName, String queueUrl) {
  		String userData = "";
  		
  		userData += "#!/bin/bash" + "\n"
  				+ "apt update" + "\n"
  				+ "apt -y install default-jre" + "\n"
  				+ "apt -y install unzip" + "\n"
  				+ "apt -y install maven" + "\n"
  				+ "apt update" + "\n"
  				+ "curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip'" + "\n"
  				+ "unzip awscliv2.zip" + "\n"
  				+ "sudo ./aws/install" + "\n"
  				+ "cd home/ubuntu" + "\n"
  				+ "aws s3 sync s3://"+bucketName+" ." + "\n"
  				+ "aws sqs send-message --queue-url "+queueUrl+" --message-body 'bootTerminated' " + "\n";
  		String base64UserData = null;
  		try {
  		    base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
  		} catch (UnsupportedEncodingException e) {
  		    e.printStackTrace();
  		}
  		return base64UserData;
	}
	
	private boolean waitUntilClusterIsReady() {

		AmazonEC2Waiters waiter = new AmazonEC2Waiters(ec2);
		
		List<String> instanceIds = new ArrayList<>();
		
		for (Instance instance: this.virtualMachines) instanceIds.add(instance.getInstanceId());

		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest().withInstanceIds(instanceIds);
		WaiterParameters<DescribeInstancesRequest> params = new WaiterParameters<DescribeInstancesRequest>(describeRequest);
		
		DescribeInstanceStatusRequest describeStatusRequest = new DescribeInstanceStatusRequest().withInstanceIds(instanceIds);
		WaiterParameters<DescribeInstanceStatusRequest> paramsStatus = new WaiterParameters<DescribeInstanceStatusRequest>(describeStatusRequest);

		try{
	    	//Wait until the instances of the cluster are effectively running
			waiter.instanceRunning().run(params);
		    System.out.println("   \u2022 The instances of the cluster are running...waiting for status checks...");
			
		} catch(AmazonServiceException | WaiterTimedOutException | WaiterUnrecoverableException e) {
			System.out.println("   \u2022 An exception is occurred during istanceRunning check.");
			return false;
		}

		try{
			//Now the instances are running, so waiting for status check
			waiter.instanceStatusOk().run(paramsStatus);
		    System.out.println("   \u2022 Status checked successfully.");
			
		} catch(AmazonServiceException | WaiterTimedOutException | WaiterUnrecoverableException e) {
			System.out.println("   \u2022 An exception is occurred during status check.");
			return false;
		}
					
		return true;
	}
	
	 protected int getVCPUsCount(String instaceType) {
		 DescribeInstanceTypesResult res = ec2.describeInstanceTypes(new DescribeInstanceTypesRequest().withInstanceTypes(instaceType));
		 return res.getInstanceTypes().get(0).getVCpuInfo().getDefaultVCpus();
	 }

}
