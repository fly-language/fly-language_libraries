package isislab.azureclient;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineEvictionPolicyTypes;
import com.microsoft.azure.management.compute.VirtualMachineSize;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.resources.fluentcore.model.Creatable;
import com.microsoft.azure.management.storage.StorageAccount;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class VMClusterHandler {
	
	private static final String FLY_VM_USER = "fly-vm-user";
	
	private Azure azure;
	private Region region;
	private boolean persistent;
	private StorageAccount sa;
	private String subscriptionId;
	protected List<VirtualMachine> virtualMachines;
	private String id;
	
	public VMClusterHandler (Azure azure, Region region, String subscriptionId, String id) {
		this.azure = azure;
		this.region = region;
		this.subscriptionId = subscriptionId;
		this.virtualMachines = new ArrayList<VirtualMachine>();
		this.id = id;
	}
	
	protected void setStorageAccount(StorageAccount sa) {
		this.sa = sa;
	}
	
	//Check if VM Size specified is an existent one
	private boolean checkCorrectVmSize(String vmSize) {
    	if (vmSize == null || "".equals(vmSize)) {
            System.out.println("\n**ERROR: VM size cannot be null or empty!");
            return false;
        }

        for (VirtualMachineSizeTypes enumEntry : VirtualMachineSizeTypes.values()) {
            if (enumEntry.toString().equals(vmSize)) {
                return true;
            }
        }

        System.out.println("\n**ERROR: VM Size chosen: " + vmSize + " not existent or not available in the region selected.");
        return false;
    }
	
	//Boot Script for VM to install software dependencies
	private final String getUserData(String terminationQueueName, String uriBlob) {
		
  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		
  		String userData = "";
  		userData += "#!/bin/bash" + "\n"
  				+ "apt update" + "\n"
  				+ "apt -y install default-jre" + "\n"
  				+ "apt -y install unzip" + "\n"
  				+ "apt -y install maven" + "\n"
  				+ "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash" + "\n"
  				+ "cd home/"+FLY_VM_USER + "\n"
  				+ "curl "+uriBlob+" --output "+projectName + "\n"
  				+ "az storage message put --content bootTerminated --queue-name "+terminationQueueName+" --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value();
  		String base64UserData = null;
  		try {
  		    base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
  		} catch (UnsupportedEncodingException e) {
  		    e.printStackTrace();
  		}
  		return base64UserData;
	}
	//Download the ZIP of the project to execute from Azure Storage Account Container
	protected void downloadExecutionFileOnVMCluster(String resourceGroupName, String uriBlob, String terminationQueueName, AsyncHttpClient httpClient, String token) throws InterruptedException, ExecutionException {
		
	    System.out.println("\n\u27A4 Downlaoding necessary files on VMs of the cluster for FLY execution...");
	    
  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		
		final String commandBody = "{\"commandId\": \"RunShellScript\",\"script\": ["
				+ "\"cd ../../../../../../home/"+FLY_VM_USER+"\","
				+ "\"curl "+uriBlob+" --output "+projectName+" 2> downloadError 1> downloadOutput\","
				+ "\"az storage blob upload -c bucket-"+id+" -f downloadError --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
				+ "\"az storage blob upload -c bucket-"+id+" -f downloadOutput --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
				+ "\"az storage message put --content downloadTerminated --queue-name "+terminationQueueName+" --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\"]"
				+"}";		
		
		List<Response> responses = new ArrayList<>();
		
		for (VirtualMachine vm : this.virtualMachines) {
		    System.out.println("   \u2022 Downloading on VM "+vm.name());

	  		//Asynchronous run command to each VM of the cluster
			Future<Response> whenResponse = httpClient.preparePost("https://management.azure.com/subscriptions/"+this.subscriptionId+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.Compute/virtualMachines/"+vm.name()+"/runCommand?api-version=2020-12-01")
						.addHeader("Authorization", "Bearer " + token)	
	  					.addHeader("Content-Type", "application/json")
						.setBody(commandBody)
						.execute();
			
			responses.add(whenResponse.get());	
		}
		
		//One command at time can be executed on a VM, so before running next commands ensure these commands are terminated
		boolean commandsInProgress = true;
		while (commandsInProgress) {
			for (Response r : responses) {
				if(r.getStatusCode() == 202) { //(Accepted)
					Future<Response> whenResponse = httpClient.prepareGet(r.getHeader("azure-asyncoperation"))
							.addHeader("Authorization", "Bearer " + token)	
		  					.addHeader("Content-Type", "application/json")
							.execute();
					
					if ( whenResponse.get().getResponseBody().contains("Provisioning succeeded")) commandsInProgress = false;
					else commandsInProgress = true;
				}else {
					System.out.println("STATUS -> "+r.getStatusCode());
				}
			}
		}

	    System.out.println("   \u2022 Commands provisioning succeded");
	}
	
	protected void buildFLYProjectOnVMCluster(String uriBlob, String terminationQueueName, AsyncHttpClient httpClient, String resourceGroupName, String token, String mainClass) throws InterruptedException, ExecutionException {
		
	    System.out.println("\n\u27A4 Project Building...");

  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		//trim the extension
  		projectName = projectName.substring(0, projectName.lastIndexOf("."));
  		
		final String commandBody = "{\"commandId\": \"RunShellScript\",\"script\": ["
				+ "\"cd ../../../../../../home/"+FLY_VM_USER+"\","
				+ "\"unzip "+projectName+"\","
				+ "\"cd "+projectName+"\","
				+ "\"mvn -T 1C install -Dmaven.test.skip -DskipTests -Dapp.mainClass="+mainClass+" 2> buildingError 1> buildingOutput\","
				+ "\"az storage blob upload -c bucket-"+id+" -f buildingError --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
				+ "\"az storage blob upload -c bucket-"+id+" -f buildingOutput --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
				+ "\"az storage message put --content buildingTerminated --queue-name "+terminationQueueName+" --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\"]"
				+"}";
		
		List<Response> responses = new ArrayList<>();

		for (VirtualMachine vm : this.virtualMachines) {
		    System.out.println("   \u2022 Building on VM "+vm.name());
	    	while (true) {
		    	Future<Response> whenResponse = httpClient.preparePost("https://management.azure.com/subscriptions/"+this.subscriptionId+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.Compute/virtualMachines/"+vm.name()+"/runCommand?api-version=2020-12-01")
							.addHeader("Authorization", "Bearer " + token)	
		  					.addHeader("Content-Type", "application/json")
							.setBody(commandBody)
							.execute();
		    	
				if (whenResponse.get().getStatusCode() == 409) {
		    		//Conflict with a previous command, retry again in a bit
		    		Thread.sleep(1000);
		    	}else {
					responses.add(whenResponse.get());
					break;
		    	}
	    	}
		}
		
		//One command at time can be executed on a VM, so before running next commands ensure these commands are terminated
		boolean commandsInProgress = true;
		while (commandsInProgress) {
			for (Response r : responses) {
				if(r.getStatusCode() == 202) {
					Future<Response> whenResponse = httpClient.prepareGet(r.getHeader("azure-asyncoperation"))
							.addHeader("Authorization", "Bearer " + token)	
		  					.addHeader("Content-Type", "application/json")
							.execute();
					
					if ( whenResponse.get().getResponseBody().contains("Provisioning succeeded")) commandsInProgress = false;
					else commandsInProgress = true;
				}else {
					System.out.println("STATUS -> "+r.getStatusCode());
				}
			}
		}
	    System.out.println("   \u2022 Commands provisioning succeded");
	}
	
	protected String checkBuildingStatus(String buildingOutputFileName) {
        String outputLog = "";
		try {
			//Read the output file in reverse because the SUCCESS or FAILURE string is at the end
	        ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(new File(buildingOutputFileName), Charset.forName("UTF-8"));
	        String line;
	        //Read last 30 lines of the file
	        int lineToRead = 30;
	        for (int i = 0; i <= lineToRead; i++) {
	            line = reverseReader.readLine();
	            if (line.contains("BUILD SUCCESS")) return null;
	            else outputLog += (lineToRead - i) + " "+ line + "\n";
	        }
		}catch (IOException e) {
			System.out.println("Error reading the file in reverse order");
			System.exit(1);
		}
		return outputLog;
	}
	
	
	protected void executeFLYonVMCluster(ArrayList<String> objectInputsString, int numberOfFunctions, 
			String uriBlob, long idExec, AsyncHttpClient httpClient, String resourceGroupName, String token, String terminationQueueName) throws Exception {

  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		//trim the extension
  		projectName = projectName.substring(0, projectName.lastIndexOf("."));

		int vmCountToUse = this.virtualMachines.size();
		if(numberOfFunctions < vmCountToUse) vmCountToUse = numberOfFunctions;
		List<Response> responses = new ArrayList<>();
		
		//FLY execution
		System.out.println("\n\u27A4 Fly execution...");

		for (int i=0; i< vmCountToUse; i++) {
			String mySplitFileName = "mySplits"+this.virtualMachines.get(i).name()+".txt";
			
			String commandBody = "{\"commandId\": \"RunShellScript\",\"script\": ["
								+ "\"cd ../../../../../../home/"+FLY_VM_USER+"\","
								+ "\"chmod -R 777 "+projectName+"\","
								+ "\"mv "+projectName+"/src-gen .\","
								+ "\"mv "+projectName+"/target/"+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar .\","
								+ "\"java -jar "+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar "+mySplitFileName+" "+idExec+" 2> executionError 1> executionOutput\","
								+ "\"az storage blob upload -c bucket-"+id+" -f executionError --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
								+ "\"az storage blob upload -c bucket-"+id+" -f executionOutput --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
								+ "\"az storage message put --content executionTerminated --queue-name "+terminationQueueName+" --account-name "+this.sa.name()+" --account-key "+this.sa.getKeys().get(0).value()+"\","
								+ "\"rm -rf ..?* .[!.]* *\"]"
								+"}";
			
			System.out.println("   \u2022 Executing on VM "+this.virtualMachines.get(i).name());
	    	while (true) {
		    	Future<Response> whenResponse = httpClient.preparePost("https://management.azure.com/subscriptions/"+this.subscriptionId+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.Compute/virtualMachines/"+this.virtualMachines.get(i).name()+"/runCommand?api-version=2020-12-01")
							.addHeader("Authorization", "Bearer " + token)	
		  					.addHeader("Content-Type", "application/json")
							.setBody(commandBody)
							.execute();
		    	
		    	if (whenResponse.get().getStatusCode() == 409) {
		    		//Conflict with a previous command, retry again in a bit
		    		System.out.println("Status code 409: conflict with previous command.");
		    		Thread.sleep(1000);
		    	}else {
					responses.add(whenResponse.get());
					break;
		    	}
	    	}
		}
		System.out.println("   \u2022 Commands launched");
    	//Ensure the commands are succeed
    	boolean commandsInProgress = true;
		while (commandsInProgress) {
			for (Response r : responses) {
				if(r.getStatusCode() == 202) {
					Future<Response> whenResponse = httpClient.prepareGet(r.getHeader("azure-asyncoperation"))
							.addHeader("Authorization", "Bearer " + token)	
		  					.addHeader("Content-Type", "application/json")
							.execute();
					
					if ( whenResponse.get().getResponseBody().contains("succeeded")) {
						commandsInProgress = false;
					    System.out.println("   \u2022 Commands provisioning succeded");
					}else if (whenResponse.get().getResponseBody().contains("failed")
							|| whenResponse.get().getResponseBody().contains("canceled")) {
						System.out.println("   \u2022 Commands provisioning failed - follow response body");
						System.out.println(whenResponse.get().getResponseBody());
						commandsInProgress = false;
					}else{
						System.out.println(whenResponse.get().getResponseBody());
						commandsInProgress = true;
					}
				}else {
					System.out.print("STATUS CODE-> "+r.getStatusCode());
					System.out.println("- STATUS TEXT-> "+r.getStatusText());
					return;
				}
			}
		}
		
		//No need to check for command provisioning , if all results are published on the results queue the execution is went well
	}
	
	protected String checkForExecutionErrors(String executionErrortFileName) throws IOException {
		Path fileName = Path.of(executionErrortFileName);
		String executionError = Files.readString(fileName);
		Files.deleteIfExists(fileName);
		if(executionError.contains("Exception") || executionError.contains("Error")) return executionError;
		else return null;
	}
	
	private boolean checkVMcharacteristics(VirtualMachine vm, String vmUser, String vmImagePublisher, 
			String vmImageOffer, String vmImageSku, String vmSize, String purchasingOption) {
		
		//Check if the requested characteristics match the existent vm
		if(	vm.osProfile().adminUsername().equals(vmUser) &&
			vm.storageProfile().imageReference().publisher().equals(vmImagePublisher) &&
			vm.storageProfile().imageReference().offer().equals(vmImageOffer) &&
			vm.storageProfile().imageReference().sku().equals(vmImageSku) &&
			vm.size().toString().equals(vmSize)) return true;
			
		
		//TO DO: check if it is SPOT or ON-DEMAND when SPOT feature will be available

		return false;
	}
	
	//Check if there is a cluster of VMs already created with the requested characteristics
	private String checkVMClusterPersistence(int vmCount, String clusterUser, boolean persistent, String clusterVMImagePublisher, 
			String clusterVMImageOffer, String clusterVMImageSku, String clusterVMSize, String purchasingOption) {
		
		//Check if there are existent VMs to compose the cluster
		List<VirtualMachine> vmsAvailable = new ArrayList<VirtualMachine>();
		for (VirtualMachine vm : azure.virtualMachines().list()) {
			if (vm.name().contains("fly-VM")) vmsAvailable.add(vm);
		}
				
		if( vmsAvailable.size() > 0) {
			//Check if the available vms have the characteristics desired
			boolean characteristicsMatching = true;
			for (VirtualMachine vm : vmsAvailable) {
				if (! checkVMcharacteristics(vm, clusterUser, clusterVMImagePublisher, clusterVMImageOffer,
						clusterVMImageSku, clusterVMSize, purchasingOption)) characteristicsMatching = false;
			}
			
			if (characteristicsMatching) {
				//The available VMs have the characteristics desired, so finally check if the number of instances requested is the same
				if (vmsAvailable.size() == vmCount) {
					this.virtualMachines = vmsAvailable;
					return "reuse";
				}else if (vmsAvailable.size() < vmCount) {
					this.virtualMachines = vmsAvailable;
					//return the number of VMs additional needed
					return ""+(vmCount - vmsAvailable.size());
				}else {
					//There are more VMs than needed in this execution, so use less VMs
				    System.out.print("\n\u27A4 Deleting resources not needed...");
					int numberOfVmToNotNeeded = vmsAvailable.size() - vmCount;
					for (int i = 0; i < numberOfVmToNotNeeded; i++) {
						deleteResourcesNotNeeded(vmsAvailable.get(i));
						vmsAvailable.remove(i);
					}
					this.virtualMachines = vmsAvailable;
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
	
	protected int createVirtualMachinesCluster(String resourceGroupName, String vmSize, String purchasingOption, 
			boolean persistent, int vmCount, String uriBlob, String terminationQueueName, AsyncHttpClient httpClient, String token, CloudStorageAccount cloudStorageAccount) throws InterruptedException, ExecutionException, URISyntaxException, StorageException {
		
    	if (!checkCorrectVmSize(vmSize)) System.exit(1);  //VM Size not correct
	    if( purchasingOption.equals("spot")) {    	
			//Spot instance not yet supported -> not supported for Azure Student Plan
			System.out.println("Spot instances not yet supported with Azure");
			System.exit(1);
	    }
	    
		String vmImagePublisher = "Canonical";
		String vmImageOffer = "UbuntuServer";
		String vmImageSku = "18.04-LTS";
		String vmPass = "flyPass00";
	    
	    //Check if a new VM Cluster has to be created or we could use an existent one
    	this.persistent = persistent;
    	boolean moreVMneeded = false;
    	String persistenceVMCluster = checkVMClusterPersistence(vmCount, FLY_VM_USER, persistent, 
    			vmImagePublisher, vmImageOffer, vmImageSku, vmSize, purchasingOption);
    	    	
    	if ( persistenceVMCluster.equals("terminate")) {
    		//Terminate the existent VM cluster because not appropriate
    		System.out.println("\n\u27A4 The existent VM Cluster hasn't the characteristics requested, so it will be terminated, and then will be created a new one.");
    		deleteResourcesAllocated(resourceGroupName, true, cloudStorageAccount);
    	} else if (persistenceVMCluster.equals("reuse")) {
    		//An existent VM Cluster with requested characteristics exists so use it
    		System.out.println("\n\u27A4 Using existent VM Cluster");
    		//Download necessary files for the execution on existent cluster
    		downloadExecutionFileOnVMCluster(resourceGroupName, uriBlob, terminationQueueName, httpClient, token);
    		return 0;
    	} else if( !persistenceVMCluster.equals("create") ) {
    		//More VMs are needed for the cluster
    		vmCount = Integer.parseInt(persistenceVMCluster);
    		moreVMneeded = true;
    	}
        
        // Prepare a batch of Creatable Virtual Machines definitions
        List<Creatable<VirtualMachine>> creatableVirtualMachines = new ArrayList<>();
		
        // Prepare Creatable Network definition [Where all the virtual machines get added to]
        Creatable<Network> creatableNetwork = azure.networks().define("fly-vn")
                .withRegion(region)
                .withExistingResourceGroup(resourceGroupName)
                .withAddressSpace("172.16.0.0/16");
        
        for (int i = 0; i < vmCount; i++) {
        	Creatable<VirtualMachine> creatableVirtualMachine;
        	if( purchasingOption.equals("on-demand")) {
                creatableVirtualMachine = azure.virtualMachines().define("fly-VM-" + UUID.randomUUID().toString())
                        .withRegion(region)
                        .withExistingResourceGroup(resourceGroupName)
                        .withNewPrimaryNetwork(creatableNetwork)
                        .withPrimaryPrivateIPAddressDynamic()
                        .withoutPrimaryPublicIPAddress()
    	    	        .withLatestLinuxImage(vmImagePublisher, vmImageOffer, vmImageSku)
    	    	        .withRootUsername(FLY_VM_USER)
    	    	        .withRootPassword(vmPass)
    	    	        .withCustomData(getUserData(terminationQueueName, uriBlob))
    	    	        .withSize(vmSize)
    	    	        .withExistingStorageAccount(this.sa); 
        	}else {
                creatableVirtualMachine = azure.virtualMachines().define("fly-VM-" + UUID.randomUUID().toString())
                        .withRegion(region)
                        .withExistingResourceGroup(resourceGroupName)
                        .withNewPrimaryNetwork(creatableNetwork)
                        .withPrimaryPrivateIPAddressDynamic()
                        .withoutPrimaryPublicIPAddress()
    	    	        .withLatestLinuxImage(vmImagePublisher, vmImageOffer, vmImageSku)
    	    	        .withRootUsername(FLY_VM_USER)
    	    	        .withRootPassword(vmPass)
    	    	        .withCustomData(getUserData(terminationQueueName, uriBlob))
    	    	        .withSize(vmSize)
    	    	        .withSpotPriority(VirtualMachineEvictionPolicyTypes.DELETE)
    	    	        .withMaxPrice(-1.0) //The price for the VM will be the current price for Azure Spot Virtual Machine or the price for a standard VM
    	    	        .withExistingStorageAccount(this.sa);
        	}
            creatableVirtualMachines.add(creatableVirtualMachine);
        }
        
        if (moreVMneeded) System.out.print("\n\u27A4 The existent cluster has not enough VMs...Creating additional "+vmCount+" VMs for the Cluster...");
        else System.out.print("\n\u27A4 Creating "+vmCount+" VMs for the Cluster...");
        
        this.virtualMachines.addAll(azure.virtualMachines().create(creatableVirtualMachines).values());

        System.out.println("Done");
        
        return vmCount;
	}
	
	private void deleteResourcesNotNeeded(VirtualMachine vmNotNeeded) {
		//Terminate useless VM in the cluster and resource related but leave active ResourceGroup and StorageAccount
		azure.virtualMachines().deleteById(vmNotNeeded.id()); //Delete VM
		azure.networkInterfaces().deleteById(vmNotNeeded.primaryNetworkInterfaceId()); //Delete VM Network Interface
		azure.disks().deleteById(vmNotNeeded.osDiskId()); //Delete VM Disk
	}

	
	protected void deleteResourcesAllocated(String resourceGroupName, boolean terminateClusterNotMatching, CloudStorageAccount cloudStorageAccount) throws URISyntaxException, StorageException {
					
		if(this.persistent && !terminateClusterNotMatching) {
			//Don't terminate the cluster, it could be reused in the next execution
		    System.out.println("\n\u27A4 Resource cleaning");
		    
		    //Empty the storage container
    		System.out.print("   \u2022 Emptying the storage container...");
    		CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
    		CloudBlobContainer container = blobClient.getContainerReference("bucket-" + id);
    		container.delete();
		    System.out.println("Done");
		    
    		System.out.println("   \u2022 The VM Cluster is still running and it is ready for the next execution.");
    		
			return;
		}
		
		if(terminateClusterNotMatching) {
			//Terminate VMs in the cluster and resource related but leave active ResourceGroup and StorageAccount
		    System.out.println("\n\u27A4 Deleting resources not appropriate");
    		System.out.print("   \u2022 Terminating VMs not needed and resource related...");
			for(VirtualMachine vm : azure.virtualMachines().list()) {
				azure.virtualMachines().deleteById(vm.id()); //Delete VM
				azure.networkInterfaces().deleteById(vm.primaryNetworkInterfaceId()); //Delete VM Network Interface
				azure.disks().deleteById(vm.osDiskId()); //Delete VM Disk
			}
		    System.out.println("Done");
			return;
		}
			
	    System.out.println("\n\u27A4 Deleting resources used");
		
		//Delete Resource group that consists of VMs and related resources
	    System.out.println("   \u2022 Deleting resource group with all resources used...");
		azure.resourceGroups().deleteByName(resourceGroupName);
		
	    System.out.println("   \u2022 The VM cluster and related resources are successfully deleted.");
	}
	
	protected int getVCPUsCount(String vmSize) {
		for (VirtualMachineSize size : azure.virtualMachines().sizes().listByRegion(region)) {
			if (size.name().equals(vmSize)) return size.numberOfCores();
		}
		return 0;
	 }

}
