package ai.yles.service.common;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Arrays;
import java.util.Objects;

public class ElasticClient {
    private String username = "elastic";
    private String password = "bdfus@1234";
    private String ip = "192.168.0.17";
    private int port = 9200;
    private static ElasticsearchClient client;

    public static synchronized ElasticsearchClient build() {
        if (Objects.isNull(client)) {
            new ElasticClient();
        }
        return client;
    }

    private ElasticClient() {
        establishConnection();
    }

    private CredentialsProvider authorize() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        return credentialsProvider;
    }

    private void establishConnection() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(ip, port))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(authorize())
                                .setDefaultHeaders(Arrays.asList(new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())))
                                .addInterceptorLast((HttpResponseInterceptor) (response, context) -> response.addHeader("X-Elastic-Product", "Elasticsearch"));
                    }
                });
        RestClient restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
    }
}
