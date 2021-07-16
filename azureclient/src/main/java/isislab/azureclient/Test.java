package isislab.azureclient;

public class Test {

	public static void main(String[] args) throws Exception {

		String clientId = "74f53ce0-e408-4267-99cf-f5eb29646e41";
		String tenantId = "f99f4e8e-d8c2-48d5-be46-3e41efda93ca";
		String secret = "";
		String subscriptionId = "1275fb95-2705-4486-a039-c1da133cb2eb";
		String region = "francecentral";
		String __id_execution = "45462";

		AzureClient cloud = new AzureClient("74f53ce0-e408-4267-99cf-f5eb29646e41",
				"f99f4e8e-d8c2-48d5-be46-3e41efda93ca", "d8pZ2SHThUxCgqrPHOCmh.LERIUcSB.74k",
				"1275fb95-2705-4486-a039-c1da133cb2eb", __id_execution + "", "France Central");
		
		
		String body = "{\"columns\":[{\"provincia\":\"Caserta\"},{\"provincia\":\"Napoli\"}]}";
		
		cloud.invokeFunction("insertAvg", body);
		/*
		AsyncHttpClient httpClient;
		DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config().setConnectTimeout(10000);
		httpClient = Dsl.asyncHttpClient(clientBuilder);
		String url = "https://flyappcloud1594394601000.azurewebsites.net/api/insertAvg?code=WCdDzjEOqJSSAtdshMjEYdaYJd74qJdVIdcvs4BJWKYHWrDVpAWPPQ==";
		
		httpClient.preparePost(url)
		.setHeader("Content-Type", "application/json").setHeader("Connection", "keep-alive").setBody(body)
		.execute(new AsyncHandler<Object>() {

			@Override
			public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
				System.out.println(responseStatus);
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
				System.out.println("Completata");
				return null;
			}

		});
		*/
	}
}
