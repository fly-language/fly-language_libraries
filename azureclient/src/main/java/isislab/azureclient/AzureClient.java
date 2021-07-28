 package isislab.azureclient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.appservice.AppServicePlan;
import com.microsoft.azure.management.appservice.FunctionApp;
import com.microsoft.azure.management.appservice.OperatingSystem;
import com.microsoft.azure.management.appservice.PricingTier;
import com.microsoft.azure.management.appservice.SkuDescription;
import com.microsoft.azure.management.resources.ResourceGroup;
import com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.microsoft.azure.management.storage.StorageAccount;
import com.microsoft.azure.management.storage.StorageAccountSkuType;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.rest.LogLevel;

import io.netty.handler.codec.http.HttpHeaders;

public class AzureClient {
	private final static Logger LOGGER = Logger.getLogger("FLY on Azure");

	private String clientId;
	private String tenantId;
	private String secret;
	private String subscriptionId;

	private String id;
	private Region region;

	private Azure azure;
	private ResourceGroup resourceGroup;
	private StorageAccount storageAccount;
	private CloudStorageAccount cloudStorageAccount;

	private String appSimpleName;
	private String appName;
	private String masterKey;

	private HashMap<String, CloudQueue> queues;
	
	private VMClusterHandler vmClusterHandler;
	private String projectID;
	private String terminationQueueName;

	private AsyncHttpClient httpClient;

	public AzureClient(String clientId, String tenantId, String secret, String subscriptionId, String id, String region)
			throws CloudException, IOException {
		DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config().setConnectTimeout(10000);
		httpClient = Dsl.asyncHttpClient(clientBuilder);
		this.clientId = clientId;
		this.tenantId = tenantId;
		this.secret = secret;
		this.subscriptionId = subscriptionId;

		this.id = id;
		this.region = Region.fromName(region);
		this.appSimpleName = "flyappazu";
		this.appName = "flyappazu" + this.id;
		queues = new HashMap<>();

		this.azure = login();
	}
	
	//VM Cluster constructor
	public AzureClient(String clientId, String tenantId, String secret, String subscriptionId, String id, String region, String terminationQueueName)
			throws CloudException, IOException {
		
		DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config().setConnectTimeout(10000);
		httpClient = Dsl.asyncHttpClient(clientBuilder);
		this.clientId = clientId;
		this.tenantId = tenantId;
		this.secret = secret;
		this.subscriptionId = subscriptionId;

		this.id = id;
		this.region = Region.fromName(region);
		this.terminationQueueName = terminationQueueName;
		queues = new HashMap<>();

		this.azure = login();
		
		//VM Cluster handling
		this.vmClusterHandler = new VMClusterHandler(this.azure, this.region, this.subscriptionId);
	}
	
	
	private Azure login() throws CloudException, IOException {
		ApplicationTokenCredentials credentials = new ApplicationTokenCredentials(clientId, tenantId, secret,
				AzureEnvironment.AZURE);
		Azure.Authenticated authenticated = Azure.configure().withLogLevel(LogLevel.BODY_AND_HEADERS)
				.authenticate(credentials);
		if (subscriptionId != null) {
			return authenticated.withSubscription(subscriptionId);
		} else {
			Azure azure = authenticated.withDefaultSubscription();
			if (azure.subscriptionId() == null) {
				throw new IllegalArgumentException("There is no default subscription");
			}
			return azure;
		}
	}

	public void init() throws InvalidKeyException, URISyntaxException {
		LOGGER.info("Preparing the necessary for azure client");
		createResourceGroup();
		createStorageAccount();
		LOGGER.info("Init finish");
	}

	private void createResourceGroup() {
		LOGGER.info("Creating resource group...");
		this.resourceGroup = azure.resourceGroups().define("flyrg" + id).withRegion(region).create();
		LOGGER.info("Resource group 'flyrg" + id + "' created");
	}

	private void createStorageAccount() throws InvalidKeyException, URISyntaxException {
		LOGGER.info("Creating storage account...");
		this.storageAccount = azure.storageAccounts().define("flysa" + id).withRegion(region)
				.withExistingResourceGroup(this.resourceGroup).withSku(StorageAccountSkuType.STANDARD_LRS).create();

		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=" + storageAccount.name()
				+ ";" + "AccountKey=" + storageAccount.getKeys().get(0).value() + ";"
				+ "EndpointSuffix=core.windows.net";
		// Connect to azure storage account
		cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
		LOGGER.info("Storage account 'flysa" + id + "' created");
	}

	public String uploadFile(java.io.File file)
			throws URISyntaxException, StorageException, InvalidKeyException, IOException {
		CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
		CloudBlobContainer container = blobClient.getContainerReference("bucket-" + id);
		container.createIfNotExists();

		BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
		containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
		container.uploadPermissions(containerPermissions);

		CloudBlockBlob blob = container.getBlockBlobReference(file.getName());
		blob.upload(new FileInputStream(file), file.length());
		return blob.getUri().toString();
	}

	public void createQueue(String name) throws Exception {
		name = name.toLowerCase();
		CloudQueueClient queueClient = cloudStorageAccount.createCloudQueueClient();
		CloudQueue queue = queueClient.getQueueReference(name);

		queue.create();
		queue.setShouldEncodeMessage(false);
		queues.put(name, queue);
	}

	public String peekFromQueue(String name) throws Exception {
		name = name.toLowerCase();
		CloudQueueMessage receivedMessage = queues.get(name).retrieveMessage();
		String result = receivedMessage.getMessageContentAsString();
		queues.get(name).deleteMessage(receivedMessage);
		System.out.println("get message from " + name);
		return result;
	}

	public List<String> peeksFromQueue(String name, int n) throws Exception {
		name = name.toLowerCase();
		List<String> values = new ArrayList<>();
		for (CloudQueueMessage message : queues.get(name).retrieveMessages(n)) {
			values.add(message.getMessageContentAsString());
			queues.get(name).deleteMessage(message);
		}
		return values;
	}

	public void addToQueue(String name, String value) throws StorageException {
		name = name.toLowerCase();
		CloudQueueMessage sentMessage = new CloudQueueMessage(value);
		queues.get(name).addMessage(sentMessage);
	}
	
	public void setupQueue(String name) throws Exception {
		
		if( !queues.containsKey(name)) {
			//queue not existent or not yet added to HasMap of queues
			name = name.toLowerCase();
			CloudQueueClient queueClient = cloudStorageAccount.createCloudQueueClient();
			CloudQueue queue = queueClient.getQueueReference(name);
			
			queue.createIfNotExists();
			queues.put(name, queue);
		}
	}
	
	public long getQueueLength(String name) throws Exception {
		CloudQueueClient queueClient = cloudStorageAccount.createCloudQueueClient();
		CloudQueue queue = queueClient.getQueueReference(name);
		
		queue.downloadAttributes();
		
		return queue.getApproximateMessageCount();
	}

	public void createFunctionApp(String name, String language) throws IOException {
		LOGGER.info("Creating function app...");
	
		if(language.contains("node") || language.contains("javascript") || language.contains("js")) {
			language = "node";
		} else if (language.contains("python")) {
			language = "python";
		}
		SkuDescription skuDescription = new SkuDescription().withName("Y1").withTier("Dynamic").withSize("Y1")
				.withFamily("Y").withCapacity(0);

		AppServicePlan appServicePlan = azure.appServices().appServicePlans().define("asp" + name + id)
				.withRegion(region).withExistingResourceGroup(resourceGroup)
				.withPricingTier(PricingTier.fromSkuDescription(skuDescription))
				.withOperatingSystem(OperatingSystem.LINUX).withPerSiteScaling(false).create();

		FunctionApp functionApp = azure.appServices().functionApps().define(name + id)
				.withExistingAppServicePlan(appServicePlan).withExistingResourceGroup(resourceGroup)
				.withExistingStorageAccount(storageAccount).withLocalGitSourceControl()
				.withAppSetting("FUNCTIONS_WORKER_RUNTIME", language)
				.withAppSetting("FUNCTIONS_EXTENSION_VERSION", "~2").create();

		this.appSimpleName = name;
		this.appName = name + id;
		// this.functionApp = functionApp;
		LOGGER.info("Function app '" + name + id + "' created");
	}

	private String getOAuthToken() throws IOException {
		URL url = new URL("https://login.microsoftonline.com/" + tenantId + "/oauth2/token");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// Set connections properties
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");

		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("grant_type", "client_credentials"));
		params.add(new BasicNameValuePair("client_id", clientId));
		params.add(new BasicNameValuePair("client_secret", secret));
		params.add(new BasicNameValuePair("resource", "https://management.azure.com/"));

		OutputStream os = connection.getOutputStream();
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		writer.write(getQuery(params));
		writer.flush();
		writer.close();
		os.close();

		Gson gson = new GsonBuilder().create();
		JsonReader jsonReader = gson.newJsonReader(new InputStreamReader(connection.getInputStream()));
		OAuthReply oAuthReply = gson.fromJson(jsonReader, OAuthReply.class);
		return oAuthReply.access_token;
	}

	private class OAuthReply {
		String access_token;
	}

	/**
	 * Builds query params for http request.
	 *
	 * @param params
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private String getQuery(List<NameValuePair> params) throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (NameValuePair pair : params) {
			if (first)
				first = false;
			else
				result.append("&");
			result.append(URLEncoder.encode(pair.getName(), "UTF-8"));
			result.append("=");
			result.append(URLEncoder.encode(pair.getValue(), "UTF-8"));
		}
		return result.toString();
	}

	private String getMasterKey() throws IOException {
		String token = getOAuthToken();

		URL url = new URL("https://management.azure.com/subscriptions/" + subscriptionId + "/resourceGroups/"
				+ resourceGroup.name() + "/providers/Microsoft.Web/sites/" + appName
				+ "/hostruntime/admin/host/systemkeys/_master?api-version=2015-08-01");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		// Set connections properties
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Authorization", "Bearer " + token);
		connection.getOutputStream().close();

		Gson gson = new GsonBuilder().create();
		JsonReader jsonReader = gson.newJsonReader(new InputStreamReader(connection.getInputStream()));
		MasterKeyReply masterKeyReply = gson.fromJson(jsonReader, MasterKeyReply.class);
		return masterKeyReply.value;
	}

	private class MasterKeyReply {
		String value;
	}

	public void clear(String pathToFunction, String pathToEnv) {
		LOGGER.info("Deleting function app...");
		// azure.appServices().functionApps().deleteById(functionApp.id());
		LOGGER.info("Function app deleted");

		LOGGER.info("Deleting storage account...");
		azure.storageAccounts().deleteById(storageAccount.id());
		LOGGER.info("Storage account deleted");

		LOGGER.info("Deleting resource group...");
		azure.resourceGroups().deleteByName(resourceGroup.name());
		LOGGER.info("Resource group deleted");

		deleteIfExists(pathToFunction);
		deleteIfExists(pathToEnv);
	}

	private void deleteIfExists(String path) {
		File file = new File(path);
		if (file.exists()) {
			try {
				FileUtils.deleteDirectory(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void publishFunction(String functionName, String script) throws IOException, InterruptedException {
		Runtime.getRuntime().exec("chmod +x " + script);
		ProcessBuilder __processBuilder = new ProcessBuilder("/bin/bash", "-c",
				script + " " + appSimpleName + " " + functionName + " " + id + " '" + clientId + "' '" + tenantId
						+ "' '" + secret + "' " + subscriptionId + " " + storageAccount.name() + " "
						+ storageAccount.getKeys().get(0).value());
		runScript(__processBuilder);
		LOGGER.info("Retrieves master key");
		int attempts = 20;
		int i = 0;
		do {
			i++;
			try {
				this.masterKey = getMasterKey();
			} catch (Exception e) {
			}
		} while (i < attempts && masterKey == null);
		LOGGER.info("Master key obtained, " + masterKey);
	}

	public ListenableFuture<Object> invokeFunction(String functionName, String body) throws IOException {
		ListenableFuture<Object> tmp = httpClient
				.preparePost("https://" + appName + ".azurewebsites.net/api/" + functionName + "?code=" + masterKey)
				.setHeader("Content-Type", "application/json").setHeader("Connection", "keep-alive").setBody(body)
				.execute(new AsyncHandler<Object>() {

					@Override
					public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public State onHeadersReceived(HttpHeaders headers) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public void onThrowable(Throwable t) {
						// TODO Auto-generated method stub

					}

					@Override
					public Object onCompleted() throws Exception {
						// TODO Auto-generated method stub
						return null;
					}

				});
		return tmp;
	}

	private void runScript(ProcessBuilder __processBuilder) throws IOException, InterruptedException {
		__processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		Map<String, String> __env = __processBuilder.environment();
		__processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
		String __path_env = __env.get("PATH");
		if (!__path_env.contains("/usr/local/bin")) {
			__env.put("PATH", __path_env + ":/usr/local/bin");
		}
		Process __p;
		__p = __processBuilder.start();
		__p.waitFor();
		if (__p.exitValue() != 0) {
			System.out.println("Error in deploy .sh ");
			System.exit(1);
		}
	}

	public String getDBEndpoint(String resourceGroupName, String instance) throws IOException {
		String token = getOAuthToken();
		
		URL url = new URL("https://management.azure.com/subscriptions/" + subscriptionId 
				+ "/resourceGroups/" + resourceGroupName 
				+ "/providers/Microsoft.DBforMySQL/servers/" + instance
				+ "?api-version=2017-12-01"
				);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();	

		// Set connections properties
		connection.setDoOutput(true);
		connection.setRequestMethod("GET");
		connection.setRequestProperty("Authorization", "Bearer " + token);
		connection.setRequestProperty("Accept", "application/json");

		Gson gson = new GsonBuilder().create();
		JsonReader jsonReader = gson.newJsonReader(new InputStreamReader(connection.getInputStream()));
		JsonObject jsonObj = new JsonParser().parse(jsonReader).getAsJsonObject();
		
		String dbEndpoint = jsonObj.getAsJsonObject("properties").get("fullyQualifiedDomainName").getAsString(); 
		
		return dbEndpoint;
	}
	
	public String getDBEndpointNoSQL(String resourceGroups, String instance) throws IOException {
		String token = getOAuthToken();
		
		URL url = new URL("https://management.azure.com/subscriptions/" + subscriptionId
				+ "/resourceGroups/" + resourceGroups + "/providers/Microsoft.DocumentDB/databaseAccounts/"
				+ instance + "/listConnectionStrings?api-version=2021-03-01-preview");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();	
				
		// Set connections properties
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Authorization", "Bearer " + token);
		connection.getOutputStream().close();
		
		Gson gson = new GsonBuilder().create();
		JsonReader jsonReader = gson.newJsonReader(new InputStreamReader(connection.getInputStream()));
		JsonObject jsonObj = new JsonParser().parse(jsonReader).getAsJsonObject();
		
		String dbEndpoint = jsonObj.getAsJsonArray("connectionStrings").get(0).getAsJsonObject().get("connectionString").getAsString();
		
		return dbEndpoint;
	}
	
	//VM Cluster handling methods
	public void VMClusterInit() throws InvalidKeyException, URISyntaxException {
		
		boolean resourceGroupFound = false;
		for (ResourceGroup rg : azure.resourceGroups().list()) {
			if (rg.name().contains("flyrg")) {
				this.resourceGroup = rg;
				resourceGroupFound = true;
			}
		}
		if(!resourceGroupFound) createResourceGroup();
		
		boolean storageAccountFound = false;
		for (StorageAccount sa : azure.storageAccounts().list()) {
			if (sa.name().contains("flysa")) {
				this.storageAccount = sa;
				
				String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=" + this.storageAccount.name()
				+ ";" + "AccountKey=" + this.storageAccount.getKeys().get(0).value() + ";"
				+ "EndpointSuffix=core.windows.net";
		
				// Connect to azure storage account
				this.cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
				
				storageAccountFound = true;
			}
		}
		if(!storageAccountFound) createStorageAccount();
	}
	
	//Upload ZIP of current project to the storage account
	public void zipAndUploadCurrentProject() 
			throws InvalidKeyException, URISyntaxException, StorageException, IOException, InterruptedException {
		
		String whereami = "";
		System.out.println("\n\u27A4 ZIP generation and upload current project to a container on Azure.");
			
		//Generate ZIP
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
				
	    //Get the file ZIP to upload
		File f = new File(whereami+".zip");
		
		System.out.print("   \u2022 Uploading ZIP file to Azure...");
		String uriBlob = uploadFile(f);
		System.out.println("Done");
		
		this.projectID = uriBlob;
	}
	 
	 public int launchVMCluster(String vmSize, String purchasingOption, boolean persistent, int vmCount) throws InterruptedException, ExecutionException, IOException {
		 vmClusterHandler.setStorageAccount(this.storageAccount);
		 return vmClusterHandler.createVirtualMachinesCluster(this.resourceGroup.name(), vmSize, purchasingOption, persistent, vmCount, this.projectID, this.terminationQueueName, this.httpClient, getOAuthToken());
	 }
	 
	 public void downloadFLYProjectonVMCluster() throws InterruptedException, ExecutionException, IOException {
		 vmClusterHandler.downloadExecutionFileOnVMCluster(this.resourceGroup.name(), this.projectID, this.terminationQueueName, this.httpClient, getOAuthToken());
	 }
	 
	 public void buildFLYProjectOnVMCluster() throws Exception {
		 vmClusterHandler.buildFLYProjectOnVMCluster(this.projectID, this.terminationQueueName, this.httpClient, this.resourceGroup.name(), getOAuthToken());
	 }
	 
	 public void executeFLYonVMCluster(int[] dimPortions, int[] displ, int numberOfFunctions, long idExec) throws Exception {
		 vmClusterHandler.executeFLYonVMCluster(dimPortions, displ, numberOfFunctions, this.projectID, idExec, this.httpClient, this.resourceGroup.name(), getOAuthToken());
	 }

	 public void deleteResourcesAllocated() {
		 vmClusterHandler.deleteResourcesAllocated(this.resourceGroup.name(), false);
	 }
}