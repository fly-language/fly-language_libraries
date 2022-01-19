package isislab.awsclient;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.json.JSONArray;

import org.apache.commons.io.input.ReversedLinesFileReader;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.model.CreateDocumentRequest;
import com.amazonaws.services.simplesystemsmanagement.model.CreateDocumentResult;
import com.amazonaws.services.simplesystemsmanagement.model.DeleteDocumentRequest;
import com.amazonaws.services.simplesystemsmanagement.model.DocumentIdentifier;
import com.amazonaws.services.simplesystemsmanagement.model.ListDocumentsRequest;
import com.amazonaws.services.simplesystemsmanagement.model.ListDocumentsResult;
import com.amazonaws.services.simplesystemsmanagement.model.SendCommandRequest;
import com.amazonaws.services.simplesystemsmanagement.model.SendCommandResult;

public class RunCommandHandler {
	
	private AWSSimpleSystemsManagement ssm;
	private S3Handler s3Handler;
	
	private List<Instance> virtualMachines;
	private HashMap<String, String> commandIds;

	protected RunCommandHandler(AWSSimpleSystemsManagement ssm, S3Handler s3Handler) {
		this.ssm = ssm;
		this.s3Handler = s3Handler;
	}
	
	protected void setVMs(List<Instance> virtualMachines) {
		this.virtualMachines = virtualMachines;
	}
	
	protected void downloadExecutionFileOnVMCluster(String bucketName, String queueUrl) {
		String syncDocName = "fly_downloading";
		
	    System.out.println("\n\u27A4 Downlaoding necessary files on VMs of the cluster for FLY execution...");
		
	    //Create the document for the command
		try {
	      createDocumentMethod(getDocumentContent4(bucketName, queueUrl), syncDocName);
	    }
	    catch (IOException e) {
	      e.printStackTrace();
	    }
				
		//Download file for FLY execution on VMs (useful when instances are already booted, if it is the first time the download will happen at the beginning	    
	    for (Instance vm : this.virtualMachines) {
			System.out.println("Downloading on VM "+vm.getInstanceId());
			executeCommand(vm.getInstanceId(), bucketName, syncDocName, "downloading");
		}
	}
		
	protected void buildFLYProjectOnVMCluster(String bucketName, String projectName, String queueUrl, String mainClass) throws InterruptedException, ExecutionException {

		String docJarName = "fly_building";
		
	    //Create the document for the command
		try {
			createDocumentMethod(getDocumentContent2(projectName, queueUrl, mainClass), docJarName);
	    }
	    catch (IOException e) {
	      e.printStackTrace();
	    }
		
		//Building and JAR generation (the output of the building is published on S3)
		System.out.println("\n\u27A4 Project building...");
		for (Instance vm : this.virtualMachines) {
			System.out.println("Building on VM "+vm.getInstanceId());
			executeCommand(vm.getInstanceId(), bucketName, docJarName, "building");
		}
	}
	
	protected String checkBuildingStatus(String bucketName) throws IOException  {
		
		//All VMs are in the same state, so I can check the building status of just one VM
		Map.Entry<String, String> hm = commandIds.entrySet().stream().findFirst().get();
		//Construct the key of "stdout" file
		String stdoutKey = "buildingStatusOutput/"+hm.getKey()+"/"+hm.getValue()+"/awsrunShellScript/building/stdout";
		
		String outputLog = "**BUILD FAILED**" + "\n";
		File buildingOutputFile = s3Handler.getS3ObjectToFile(bucketName, stdoutKey);
		if( buildingOutputFile != null) {
			ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(buildingOutputFile, Charset.forName("UTF-8"));
	        String line;
	        //Read last 30 lines of the file
	        int lineToRead = 30;
	        for (int i = 0; i < lineToRead; i++) {
	            line = reverseReader.readLine();
	            if (line.contains("BUILD SUCCESS")) {
	            	System.out.println("BUILD SUCCESS");
	            	buildingOutputFile.delete();
	    	        reverseReader.close();
	            	return null;
	            }else outputLog += (lineToRead - i) + " "+ line + "\n";
	        }
			//Error in building
	        reverseReader.close();
	    	buildingOutputFile.delete();
			return outputLog;
		}else return "errorReadingFile";
	}

	protected void executeFLYonVMCluster(ArrayList<String> objectInputsString, ArrayList<String> constVariables, int numberOfFunctions, 
			String bucketName, String projectName, long idExec, String queueUrl) throws InterruptedException, ExecutionException {

		String docExecutionName = "fly_execution";
		int vmCount = this.virtualMachines.size();
		
		//using @@@ as special string (this could be a problem if there is this @@@ in the strings element) -> a better solution should be
		//post this object on cloud and then take it from there already formatted
		String constVariablesString = new String();
		for (String c : constVariables) constVariablesString = constVariablesString + "@@@" + c;
		
		//Check if the input is just a range of functions to execute
		if(objectInputsString.get(0).contains("portionRangeLength")) {
			//Range input
			
			//Create the document for the command
			try {
				for (int i=0; i < vmCount; i++) {
					createDocumentMethod(getDocumentContent3(projectName,bucketName,objectInputsString.get(i), constVariablesString, idExec, queueUrl), 
						docExecutionName+this.virtualMachines.get(i).getInstanceId());
				}
		    }
		    catch (IOException e) {
		      e.printStackTrace();
		    }
			
		}else {
			//Array or matrix split input
			//Specify how many splits each VM has to compute
			int splitsNum = objectInputsString.size();
			
			int[] splitCount = new int[vmCount];
			int[] displ = new int[vmCount]; 
			int offset = 0;
			
			for(int i=0; i < vmCount; i++) {
				splitCount[i] = ( splitsNum / vmCount) + ((i < (splitsNum % vmCount)) ? 1 : 0);
				displ[i] = offset;
				offset += splitCount[i];
			}
			
		    //Create the document for the command
			try {
				for (int i=0; i < vmCount; i++) {
					//Select my part of splits
					String mySplits = splitCount[i] + "";
					for(int k=displ[i]; k < displ[i] + splitCount[i]; k++) mySplits = mySplits + "@@@" + objectInputsString.get(k);
					
					createDocumentMethod(getDocumentContent3(projectName,bucketName,mySplits,constVariablesString, idExec, queueUrl), 
						docExecutionName+this.virtualMachines.get(i).getInstanceId());
				}
		    }
		    catch (IOException e) {
		      e.printStackTrace();
		    }
		}
		
		//FLY execution
		System.out.println("\n\u27A4 Fly execution...");		
		for (int i=0; i< vmCount; i++) {
			
			System.out.println("Running on VM "+this.virtualMachines.get(i).getInstanceId());
			executeCommand(this.virtualMachines.get(i).getInstanceId(), bucketName, docExecutionName+this.virtualMachines.get(i).getInstanceId(), "execution");
		}
	}
	
	private void executeCommand(String instanceId, String bucketName, String commandsDocName, String folderBucketOutput) {
				
		//Send the commands described in the given document
		SendCommandRequest sendCommandRequest = new SendCommandRequest()
				.withInstanceIds(instanceId)
				.withDocumentName(commandsDocName);
		
		if (folderBucketOutput.equals("building")) {
			sendCommandRequest.setOutputS3BucketName(bucketName);
			sendCommandRequest.setOutputS3KeyPrefix("buildingStatusOutput/");
		}else if (folderBucketOutput.equals("execution")) {
			sendCommandRequest.setOutputS3BucketName(bucketName);
			sendCommandRequest.setOutputS3KeyPrefix("FLYexecutionOutput/");
		}else {
			//Downloading command
			sendCommandRequest.setOutputS3BucketName(bucketName);
			sendCommandRequest.setOutputS3KeyPrefix("downloadingOutput/");
		}
		
		SendCommandResult commandResult = ssm.sendCommand(sendCommandRequest);
		//store info about command id for the next check of errors
		commandIds = new HashMap<>();
		commandIds.put(commandResult.getCommand().getCommandId(), instanceId);
	}
	
	protected String checkForExecutionErrors(String bucketName) {
		
		//All VMs are in the same state, so I can check the execution status of just one VM
		Map.Entry<String, String> hm = commandIds.entrySet().stream().findFirst().get();
		//Construct the key "stderr" file
		String stderrKey = "FLYexecutionOutput/"+hm.getKey()+"/"+hm.getValue()+"/awsrunShellScript/execution/stderr";
		
		String error = s3Handler.getS3ObjectToString(bucketName, stderrKey);
		
		if(error.contains("Exception") || error.contains("Error")) return error;
		else return null;
	}

	protected void deleteFLYdocumentsCommand() {
		List<String> docNameToDelete = new ArrayList<>();
		String nextToken = null;

	    do {
			ListDocumentsRequest request = new ListDocumentsRequest().withNextToken(nextToken);
			ListDocumentsResult results = ssm.listDocuments(request);
			
			List<DocumentIdentifier> docs = results.getDocumentIdentifiers();

		    for (DocumentIdentifier doc : docs) {
		    	if(doc.getName().contains("fly")) docNameToDelete.add(doc.getName());
		    }
		    nextToken = results.getNextToken();
	    } while (nextToken != null);

	    for (String docName : docNameToDelete) {
			DeleteDocumentRequest deleteDocRequest = new DeleteDocumentRequest().withName(docName);
			ssm.deleteDocument(deleteDocRequest);
	    }
	}
	
	//Command to run "mvn install" on the FLY project to execute
	private static String getDocumentContent2(String projectName, String queueUrl, String mainClass) throws IOException {
		return "---" + "\n"
			+ "schemaVersion: '2.2'" + "\n"
			+ "description: Building and JAR generation." + "\n"
			+ "parameters: {}" + "\n"
			+ "mainSteps:" + "\n"
			+ "- action: aws:runShellScript" + "\n"
			+ "  name: building" + "\n"
			+ "  inputs:" + "\n"
			+ "    runCommand:" + "\n"
			+ "    - cd ../../../../home/ubuntu"+ "\n"
			+ "    - unzip "+projectName+ "\n"
			+ "    - cd "+projectName+ "\n"
			+ "    - mvn -T 1C install -Dmaven.test.skip -DskipTests -Dapp.mainClass="+mainClass+ "\n"
			+ "    - aws sqs send-message --queue-url "+queueUrl+" --message-body buildingTerminated"+ "\n";
	}

	//Run FLY execution
	private static String getDocumentContent3(String projectName, String bucketName, String objectInputString,
			String constVariables, long idExec, String queueUrl) throws IOException {
		return "---" + "\n"
			+ "schemaVersion: '2.2'" + "\n"
			+ "description: Execute FLY application." + "\n"
			+ "parameters: {}" + "\n"
			+ "mainSteps:" + "\n"
			+ "- action: aws:runShellScript" + "\n"
			+ "  name: execution" + "\n"
			+ "  inputs:" + "\n"
			+ "    runCommand:" + "\n"
			+ "    - cd ../../../../home/ubuntu"+ "\n"
			+ "    - chmod -R 777 "+projectName+ "\n"
			+ "    - mv "+projectName+"/src-gen ."+ "\n"
			+ "    - mv "+projectName+"/target/"+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar ."+ "\n"
			+ "    - java -jar "+projectName+"-0.0.1-SNAPSHOT-jar-with-dependencies.jar "+objectInputString+" "+constVariables+" "+idExec+ "\n"
			+ "    - rm -rf ..?* .[!.]* *"+ "\n" //delete all files (also the hidden ones)
			+ "    - aws sqs send-message --queue-url "+queueUrl+" --message-body executionTerminated"+ "\n";
	}
	
	//Download necessary files for FLY execution
	private static String getDocumentContent4(String bucketName, String queueUrl) throws IOException {
		return "---" + "\n"
			+ "schemaVersion: '2.2'" + "\n"
			+ "description: Syncing S3 Bucket." + "\n"
			+ "parameters: {}" + "\n"
			+ "mainSteps:" + "\n"
			+ "- action: aws:runShellScript" + "\n"
			+ "  name: syncing" + "\n"
			+ "  inputs:" + "\n"
			+ "    runCommand:" + "\n"
			+ "    - cd ../../../../home/ubuntu"+ "\n"
			+ "    - aws s3 sync s3://"+bucketName+" ." + "\n"
			+ "    - aws sqs send-message --queue-url "+queueUrl+" --message-body downloadTerminated"+ "\n";
	}
	
	private void createDocumentMethod (final String documentContent, String docName) {
	      final CreateDocumentRequest createDocRequest = new CreateDocumentRequest()
	    		  .withContent(documentContent)
	    		  .withName(docName)
	    		  .withDocumentType("Command")
	    		  .withDocumentFormat("YAML"); //The alternative is JSON
	      
	      final CreateDocumentResult result = ssm.createDocument(createDocRequest);
	    }

}
