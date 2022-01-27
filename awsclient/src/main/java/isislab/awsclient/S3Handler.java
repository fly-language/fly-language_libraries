package isislab.awsclient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Handler {

	private AmazonS3 s3;

	protected S3Handler (AmazonS3 s3) {
		this.s3 = s3;
	}

	protected String createBucket(String bucketName) {

		System.out.println("\u27A4 S3 bucket setting up...");

		Bucket b = null;
		try {
			if (s3.doesBucketExistV2(bucketName)) {
				b = getBucket(bucketName);
				if (b != null) {
					System.out.format("   \u2022 Taking the bucket %s already existent.\n", bucketName);
				}else {
					System.out.format("   \u2022 Bucket  name %s is already used, try another name.\n", bucketName);
					return "";
				}
			} else {
				b = s3.createBucket(bucketName);
				System.out.println("   \u2022 Bucket created.");
			}
		}catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}

		return b.getName();
	}

	private Bucket getBucket(String bucketName) {
		Bucket named_bucket = null;
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			if (b.getName().equals(bucketName)) {
				named_bucket = b;
			}
		}
		return named_bucket;
	}

	protected String uploadCurrentProject(String bucketName) throws IOException {

		String whereami = "";
		System.out.println("\n\u27A4 ZIP generation and upload to AWS S3");

		//Generate ZIP
		try{
			System.out.print("   \u2022 ZIP generation...");

			//Get the project folder name
			Process p = Runtime.getRuntime().exec("pwd");
			BufferedReader p_output2 = new BufferedReader(new InputStreamReader(p.getInputStream()));
			whereami = p_output2.readLine();
			whereami = whereami.substring(whereami.lastIndexOf(File.separator) + 1);

			//ZIP generation
			p = Runtime.getRuntime().exec("zip -r "+whereami+".zip ../"+whereami);
			p.waitFor();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//Get the file ZIP to upload
		File f = new File(whereami+".zip");

		System.out.print("   \u2022 Uploading ZIP file to AWS S3...");
		s3.putObject(new PutObjectRequest(bucketName, f.getName(), f));
		Files.delete(Path.of(whereami+".zip"));
		System.out.println("Done");

		return whereami;
	}

	protected void deleteBucketWithItsContent(String bucketName, boolean justEmpty) {

		if (justEmpty) {
			//Just empty the bucket
			try {
				System.out.print("   \u2022 Removing objects from bucket...");
				ObjectListing object_listing = s3.listObjects(bucketName);
				while (true) {
					for (Iterator<?> iterator =
							object_listing.getObjectSummaries().iterator();
							iterator.hasNext(); ) {
						S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
						s3.deleteObject(bucketName, summary.getKey());
					}

					// more object_listing to retrieve?
					if (object_listing.isTruncated()) {
						object_listing = s3.listNextBatchOfObjects(object_listing);
					} else {
						break;
					}
				}

			} catch (AmazonServiceException e) {
				System.err.println(e.getErrorMessage());
				System.exit(1);
			}
			System.out.println("Done");
		}else {
			System.out.println("   \u2022 Deleting S3 bucket: " + bucketName+ "...");

			try {
				System.out.println("   \u2022 Removing objects from bucket...");
				ObjectListing object_listing = s3.listObjects(bucketName);
				while (true) {
					for (Iterator<?> iterator =
							object_listing.getObjectSummaries().iterator();
							iterator.hasNext(); ) {
						S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
						s3.deleteObject(bucketName, summary.getKey());
					}

					// more object_listing to retrieve?
					if (object_listing.isTruncated()) {
						object_listing = s3.listNextBatchOfObjects(object_listing);
					} else {
						break;
					}
				}

				s3.deleteBucket(bucketName);
			} catch (AmazonServiceException e) {
				System.err.println(e.getErrorMessage());
				System.exit(1);
			}
			System.out.println("   \u2022 S3 bucket succesfully deleted.");
		}
	}

	protected File getS3ObjectToFile(String bucketName, String objectKey) throws IOException {
		//make 10 tries (at max) because the file could not ready yet
		for (int i=0; i<10; i++) {
			if (s3.doesObjectExist(bucketName, objectKey)) {
				InputStream in = s3.getObject(new GetObjectRequest(bucketName, objectKey)).getObjectContent();
				File f = new File("buildingOutput");
				Files.copy(in, Paths.get("buildingOutput"));
				return f;
			}
		}
		return null;
	}

	protected String getS3ObjectToString(String bucketName, String objectKey) {
		//make 10 tries (at max) because the file could not ready yet
		for (int i=0; i<10; i++) {
			if (s3.doesObjectExist(bucketName, objectKey)) {
				return s3.getObjectAsString(bucketName, objectKey);
			}
		}
		return null;
	}
	
	protected void uploadFileToS3(String bucketName, File f) {
		s3.putObject(new PutObjectRequest(bucketName, f.getName(), f));
	}

	protected void writeInputObjectsToFileAndUploadToS3(ArrayList<String> objectInputsString, ArrayList<String> constVariables, 
			List<Instance> virtualMachines, int vmCountToUse, String bucketName) throws IOException {

		//write file with const vars values
		File fout = new File("constValues.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		if (constVariables.size() > 0) for (String c : constVariables) {
			bw.write(c);
			bw.newLine();
		}else bw.write("None");
		
		uploadFileToS3(bucketName, fout);
		Files.delete(Path.of(fout.getName()));

		bw.close();

		//Check if the input is just a range of functions to execute
		if(objectInputsString.get(0).contains("portionRangeLength")) {
			//Range input

			//write files with splits input for each vm
			for (int i=0; i < vmCountToUse; i++) {

				fout = new File("mySplits"+virtualMachines.get(i).getInstanceId()+".txt");
				fos = new FileOutputStream(fout);

				bw = new BufferedWriter(new OutputStreamWriter(fos));

				bw.write(objectInputsString.get(i));
				bw.newLine();
				
				uploadFileToS3(bucketName, fout);
				Files.delete(Path.of(fout.getName()));

				bw.close();
			}
		}else {
			//Array or matrix split input
			//Specify how many splits each VM has to compute
			int splitsNum = objectInputsString.size();

			int[] splitCount = new int[vmCountToUse];
			int[] displ = new int[vmCountToUse]; 
			int offset = 0;

			for(int i=0; i < vmCountToUse; i++) {
				splitCount[i] = ( splitsNum / vmCountToUse) + ((i < (splitsNum % vmCountToUse)) ? 1 : 0);
				displ[i] = offset;
				offset += splitCount[i];
			}

			for (int i=0; i < vmCountToUse; i++) {

				fout = new File("mySplits"+virtualMachines.get(i).getInstanceId()+".txt");
				fos = new FileOutputStream(fout);

				bw = new BufferedWriter(new OutputStreamWriter(fos));

				bw.write(splitCount[i]);
				//Select my part of splits
				for(int k=displ[i]; k < displ[i] + splitCount[i]; k++) {
					bw.write(objectInputsString.get(k));
					bw.newLine();
				}
				
				uploadFileToS3(bucketName, fout);
				Files.delete(Path.of(fout.getName()));

				bw.close();
			}
		}
	}

}
