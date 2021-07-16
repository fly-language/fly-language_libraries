package isislab.azureclient;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.compute.VirtualMachineEvictionPolicyTypes;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.resources.fluentcore.model.Creatable;
import com.microsoft.azure.management.storage.StorageAccount;

public class VMClusterHandler {
	
	private static final String FLY_VM_USER = "fly-vm-user";
	
	private Azure azure;
	private Region region;
	private boolean persistent;
	private StorageAccount sa;
	private String subscriptionId;
	private List<VirtualMachine> virtualMachines;
	
	public VMClusterHandler (Azure azure, Region region, String subscriptionId) {
		this.azure = azure;
		this.region = region;
		this.subscriptionId = subscriptionId;
		this.virtualMachines = new ArrayList<VirtualMachine>();
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
				+ "\"curl "+uriBlob+" --output "+projectName+"\","
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
	
	protected void buildFLYProjectOnVMCluster(String uriBlob, String terminationQueueName, AsyncHttpClient httpClient, String resourceGroupName, String token) throws InterruptedException, ExecutionException {
		
	    System.out.println("\n\u27A4 Project Building...");

  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		//trim the extension
  		projectName = projectName.substring(0, projectName.lastIndexOf("."));
  		
		final String commandBody = "{\"commandId\": \"RunShellScript\",\"script\": ["
				+ "\"cd ../../../../../../home/"+FLY_VM_USER+"\","
				+ "\"unzip "+projectName+"\","
				+ "\"cd "+projectName+"\","
				+ "\"mvn -T 1C install -Dmaven.test.skip -DskipTests -Dapp.mainClass="+projectName+"SingleVM\","
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
	
	
	//Building project and FLY execution
	protected void executeFLYonVMCluster(int[] dimPortions, int[] displ, int numberOfFunctions, String uriBlob, long idExec, AsyncHttpClient httpClient, String resourceGroupName, String token) throws InterruptedException, ExecutionException {

  		//extract project name
  		String projectName = uriBlob.substring(uriBlob.lastIndexOf("/")+1);
  		//trim the extension
  		projectName = projectName.substring(0, projectName.lastIndexOf("."));

		//FLY execution
		System.out.println("\n\u27A4 Fly execution...");
		
		List<Response> responses = new ArrayList<>();

		for (int i = 0; i< this.virtualMachines.size(); i++) {				
			
			String commandBody = "{\"commandId\": \"RunShellScript\",\"script\": ["
					+ "\"cd ../../../../../../home/"+FLY_VM_USER+"\","
					+ "\"chmod -R 777 "+projectName+"\","
					+ "\"mv "+projectName+"/src-gen .\","
					+ "\"mv "+projectName+"/target/"+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar .\","
					+ "\"java -jar "+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar "+dimPortions[i]+" "+displ[i]+" "+idExec+"\","
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
		    		Thread.sleep(1000);
		    	}else {
					responses.add(whenResponse.get());
					break;
		    	}
	    	}
		}
		
		//No need to check for command provisioning , if all results are published on the results queue the execution is went well
	    System.out.println("   \u2022 Commands provisioning succeded");
	}
	
	private boolean checkVMcharacteristics(VirtualMachine vm, String vmUser, String vmImagePublisher, 
			String vmImageOffer, String vmImageSku, String vmSize, String purchasingOption) {
		
		//Check if the requested characteristics match the existent vm
		if(	vm.osProfile().adminUsername().equals(vmUser) &&
			vm.storageProfile().imageReference().publisher().equals(vmImagePublisher) &&
			vm.storageProfile().imageReference().offer().equals(vmImageOffer) &&
			vm.storageProfile().imageReference().sku().equals(vmImageSku) &&
			vm.size().toString().equals(vmSize)) return true;
		
		//TO DO: check is is SPOT or ON-DEMAND when SPOT feature will be available

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
			boolean persistent, int vmCount, String uriBlob, String terminationQueueName, AsyncHttpClient httpClient, String token) throws InterruptedException, ExecutionException {
		
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
    		deleteResourcesAllocated(resourceGroupName, true);
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

	
	protected void deleteResourcesAllocated(String resourceGroupName, boolean terminateClusterNotMatching) {
					
		if(this.persistent && !terminateClusterNotMatching) {
			//Don't terminate the cluster, it could be reused in the next execution
    		System.out.println("\n\u27A4 The VM Cluster is still running and it is ready for the next execution.");
			return;
		}
		
		if(terminateClusterNotMatching) {
			//Terminate VMs in the cluster and resource related but leave active ResourceGroup and StorageAccount
		    System.out.print("\n\u27A4 Deleting resources not appropriate...");
			for(VirtualMachine vm : azure.virtualMachines().list()) {
				azure.virtualMachines().deleteById(vm.id()); //Delete VM
				azure.networkInterfaces().deleteById(vm.primaryNetworkInterfaceId()); //Delete VM Network Interface
				azure.disks().deleteById(vm.osDiskId()); //Delete VM Disk
			}
		    System.out.println("Done");
			return;
		}
			
	    System.out.println("\n\u27A4 Deleting resources used...");
		
		//Delete Resource group that consists of VMs and related resources
		azure.resourceGroups().deleteByName(resourceGroupName);
		
	    System.out.println("   \u2022 The VM cluster and related resources are successfully deleted.");
	}

}
