package isislab.awsclient;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;

public class SpotInstancesHandler {
	
	private AmazonEC2 ec2;
        
    protected SpotInstancesHandler (AmazonEC2 ec2) {
        this.ec2 = ec2;
    }
    
    protected List<Instance> createSpotInstances(String amiId, String securityGroupName, String keyPairName, String instance_type, 
			String bucketName, IamInstanceProfileSpecification iamInstanceProfile, int vmCount, String queueUrl) {
    	
    	try {
	    	// Submit the spot requests.
			final List<String> spotRequestIds = submitSpotRequest(securityGroupName, amiId, InstanceType.fromValue(instance_type),
				keyPairName, bucketName, iamInstanceProfile, vmCount, queueUrl);
			
			// Wait until the spot requests are in the active state
	        // (or at least not in the open state).
	        do
	        {
	            Thread.sleep(2000);
	        } while (areOpen(spotRequestIds));
	        
	        //Loop ASYNC to check for interruption notice of spot instance
	        Thread newThread = new Thread(() -> {
	            
	            while(true) {
	                if (checkForInterruption(spotRequestIds)) {
	                	System.out.println("**Spot Instance marked for termination...Your instance will be interrupted soon.");
	                	System.exit(1);
	                }else {
	                	try {
	                		//check again in 1 minute
							Thread.sleep(60000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	                }
	            }
	        });
	        newThread.start();
	        
			System.out.println("\n\u27A4 Creating and starting VM Cluster (spot) on AWS");
	        return getSpotInstances(spotRequestIds);
		
	    } catch (AmazonServiceException ase) {
	        // Write out any exceptions that may have occurred.
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
	    } catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	return null;
    }
    
	private List<String> submitSpotRequest(String secuirtyGroupName, String amiId, InstanceType instanceType,
	    	String keyPairName, String bucketName, IamInstanceProfileSpecification iamInstanceProfile, int vmCount, String queueUrl) {
		
		//Look at the Spot Instance Price History to determine the bid for the Spot Instance
		
		/*Strategy: Bid at the upper end of the range of Spot Prices (which are still below the On-Demand price), 
		 * anticipating that your one-time Spot request would most likely be fulfilled and run for enough consecutive 
		 * compute time to complete the job.
		 */
		
		DescribeSpotPriceHistoryRequest spotPriceHistoryRequest = new DescribeSpotPriceHistoryRequest()
				.withInstanceTypes(instanceType)
				.withProductDescriptions("Linux/UNIX");

		DescribeSpotPriceHistoryResult spotPriceHistoryResult = ec2.describeSpotPriceHistory(spotPriceHistoryRequest);
		
		String latestSpotPrice = spotPriceHistoryResult.getSpotPriceHistory().get(spotPriceHistoryResult.getSpotPriceHistory().size()-1).getSpotPrice();
		
        // Initializes a Spot Instance Request
        RequestSpotInstancesRequest spotRequest = new RequestSpotInstancesRequest()
        		.withSpotPrice(latestSpotPrice)
        		.withInstanceCount(vmCount);
	
        // Setup the specifications of the launch.
        LaunchSpecification launchSpecification = new LaunchSpecification();
        launchSpecification.setImageId(amiId);
        launchSpecification.setInstanceType(instanceType);
        launchSpecification.setKeyName(keyPairName);
        launchSpecification.setUserData(EC2Handler.getUserData(bucketName, queueUrl));
        launchSpecification.setIamInstanceProfile(iamInstanceProfile);
	        
	
        // Add the security group to the request.
        ArrayList<String> securityGroups = new ArrayList<String>();
        securityGroups.add(secuirtyGroupName);
        launchSpecification.setSecurityGroups(securityGroups);
	
        // Add the launch specifications to the request.
        spotRequest.setLaunchSpecification(launchSpecification);
	
        // Call the RequestSpotInstance API.
        RequestSpotInstancesResult spotResult = ec2.requestSpotInstances(spotRequest);
        List<SpotInstanceRequest> spotResponses = spotResult.getSpotInstanceRequests();
	        
	    ArrayList<String> spotInstanceRequestIds = new ArrayList<String>();
	
        for (SpotInstanceRequest requestResponse : spotResponses) {
            spotInstanceRequestIds.add(requestResponse.getSpotInstanceRequestId());
        }
	        
	    return spotInstanceRequestIds;
    }
	
	private boolean areOpen(List<String> spotInstanceRequestIds) {
	
	    // Create the describeRequest with the request id to monitor 
	    DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest()
    		    		.withSpotInstanceRequestIds(spotInstanceRequestIds);
	
	    try
	    {
	        DescribeSpotInstanceRequestsResult describeResult = ec2.describeSpotInstanceRequests(describeRequest);
	        List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();
	
	        // Look for the request and determine if it is in the active state.
	        for (SpotInstanceRequest describeResponse : describeResponses) {
	        	if (spotInstanceRequestIds.contains(describeResponse.getSpotInstanceRequestId()) &&
	        			describeResponse.getState().equals("open")) {
	                return true;
	            }
	        }
	        
	    } catch (AmazonServiceException e) {
	        // Print out the error.
	        System.out.println("Error when calling describeSpotInstances");
	        System.out.println("Caught Exception: " + e.getMessage());
	        System.out.println("Reponse Status Code: " + e.getStatusCode());
	        System.out.println("Error Code: " + e.getErrorCode());
	        System.out.println("Request ID: " + e.getRequestId());
	
	        // If we have an exception, ensure we don't break out of the loop.
	        // This prevents the scenario where there was blip on the wire.
	        return true;
	    }
	
	    return false;
	}
		
	private List<Instance> getSpotInstances(List<String> spotInstanceRequestIds) {
	
	    // Create the describeRequest with the spot request  
	    DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest()
    		    		.withSpotInstanceRequestIds(spotInstanceRequestIds);
    		    		
    	// Initialize variables.
		List<String> spotInstancesIds = new ArrayList<>();
		List<Instance> spotInstances = new ArrayList<>();

		
	    try {
	        DescribeSpotInstanceRequestsResult describeResult = ec2.describeSpotInstanceRequests(describeRequest);
	        List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();
	
	        for (SpotInstanceRequest describeResponse : describeResponses) {
	        	spotInstancesIds.add(describeResponse.getInstanceId()); 
	        }
	        
			DescribeInstancesResult describeInstancesRes = ec2.describeInstances();
			List<Reservation> reservations = describeInstancesRes.getReservations();
				
			for (Reservation reservation : reservations) {
			    for (Instance instance : reservation.getInstances()) {
			    	if (spotInstancesIds.contains(instance.getInstanceId())) spotInstances.add(instance);
			    }
			}
							
	    } catch (AmazonServiceException e) {
	        // Print out the error.
	        System.out.println("Error when calling describeSpotInstances");
	        System.out.println("Caught Exception: " + e.getMessage());
	        System.out.println("Reponse Status Code: " + e.getStatusCode());
	        System.out.println("Error Code: " + e.getErrorCode());
	        System.out.println("Request ID: " + e.getRequestId());
	    }
		    
		return spotInstances;
	}
	    		    		
		
	private boolean checkForInterruption(List<String> spotInstanceRequestIds) {
	
	    // Create the describeRequest with the spot request  
	    DescribeSpotInstanceRequestsRequest describeRequest = new DescribeSpotInstanceRequestsRequest()
    		    		.withSpotInstanceRequestIds(spotInstanceRequestIds);
	
	    try
	    {
	        DescribeSpotInstanceRequestsResult describeResult = ec2.describeSpotInstanceRequests(describeRequest);
	        List<SpotInstanceRequest> describeResponses = describeResult.getSpotInstanceRequests();
	
	        // Look for the request and determine its state.
	        for (SpotInstanceRequest describeResponse : describeResponses) {
	        	if (spotInstanceRequestIds.contains(describeResponse.getSpotInstanceRequestId()) &&
            		describeResponse.getStatus().getCode().equals("marked-for-termination")) return true;
	        }
	        
	    } catch (AmazonServiceException e) {
	        // Print out the error.
	        System.out.println("Error when calling describeSpotInstances");
	        System.out.println("Caught Exception: " + e.getMessage());
	        System.out.println("Reponse Status Code: " + e.getStatusCode());
	        System.out.println("Error Code: " + e.getErrorCode());
	        System.out.println("Request ID: " + e.getRequestId());
	
	        // If we have an exception, ensure we don't break out of the loop.
	        // This prevents the scenario where there was blip on the wire.
	        return true;
	    }
	
	    return false;
	}


}
