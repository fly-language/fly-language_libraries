package isislab.awsclient;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.CreateRoleResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;

public class AWSClient {

	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AWSSimpleSystemsManagement ssm;
	private static AmazonIdentityManagement iamClient;
	private EC2Handler ec2Handler;
	private S3Handler s3Handler;
	private RunCommandHandler runCommandHandler;

	private String bucketName;
	private String terminationQueueUrl;
	private String projectID;

	public AWSClient(BasicAWSCredentials creds, String region) {

		ec2 = AmazonEC2ClientBuilder.standard()
				.withRegion(region)
				.withCredentials(new AWSStaticCredentialsProvider(creds))
				.build();

		s3 = AmazonS3ClientBuilder.standard()
				.withRegion(region)
				.withCredentials(new AWSStaticCredentialsProvider(creds))
				.build();

		ssm = AWSSimpleSystemsManagementClientBuilder.standard()
				.withRegion(region)							 
				.withCredentials(new AWSStaticCredentialsProvider(creds))
				.build();

		iamClient = AmazonIdentityManagementClientBuilder.standard()
				.withRegion(region)							 
				.withCredentials(new AWSStaticCredentialsProvider(creds))
				.build();

		this.s3Handler = new S3Handler(s3);
		this.runCommandHandler = new RunCommandHandler(ssm, this.s3Handler);
		this.ec2Handler = new EC2Handler(ec2, iamClient, this.runCommandHandler, this.s3Handler);

	}

	public void setupTerminationQueue(String terminationQueueUrl) {
		this.terminationQueueUrl = terminationQueueUrl;
	}
	
	public void setupS3Bucket(String bucketName) {
		this.bucketName = s3Handler.createBucket(bucketName); //flybucketvmcluster
	}

	public File downloadS3ObjectToFile(String fileName) throws IOException {
		return s3Handler.getS3ObjectToFile(this.bucketName, fileName, fileName);
	}

	public void zipAndUploadCurrentProject() throws IOException {
		this.projectID = s3Handler.uploadCurrentProject(this.bucketName);
	}

	public int launchVMCluster(String instance_type, String purchasingOption, boolean persistent, int vmCount) throws InterruptedException, ExecutionException {
		return ec2Handler.createVirtualMachinesCluster(instance_type, this.bucketName, purchasingOption, persistent, vmCount, this.terminationQueueUrl);			
	}

	public int getVCPUsCount(String instaceType) {
		return ec2Handler.getVCPUsCount(instaceType);
	}

	public void downloadFLYProjectonVMCluster() throws InterruptedException {
		runCommandHandler.downloadExecutionFileOnVMCluster(this.bucketName, this.terminationQueueUrl);
	}

	public void buildFLYProjectOnVMCluster(String mainClass) throws InterruptedException, ExecutionException {
		runCommandHandler.buildFLYProjectOnVMCluster(this.bucketName, this.projectID, this.terminationQueueUrl, mainClass);
	}

	public String checkBuildingStatus() throws IOException {
		return runCommandHandler.checkBuildingStatus(this.bucketName);
	}

	public void executeFLYonVMCluster(ArrayList<String> objectInputsString, int numberOfFunctions, long idExec) throws InterruptedException, ExecutionException, IOException {
		runCommandHandler.deleteFLYdocumentsCommand();
		runCommandHandler.executeFLYonVMCluster(objectInputsString, numberOfFunctions, this.bucketName, this.projectID, idExec, this.terminationQueueUrl);
	}

	public String checkForExecutionErrors() {
		return runCommandHandler.checkForExecutionErrors(this.bucketName);
	}

	public void cleanResources() {
		runCommandHandler.deleteFLYdocumentsCommand();
	}

	public void deleteResourcesAllocated() {
		ec2Handler.deleteResourcesAllocated(false, this.bucketName);
	}
}
