package asaintsever.httpsinkconnector.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import asaintsever.httpsinkconnector.event.formatter.IEventFormatter;
import asaintsever.httpsinkconnector.http.authentication.AuthException;
import asaintsever.httpsinkconnector.http.authentication.IAuthenticationProvider;


public class HttpEndpoint {
    
    private static final Logger log = LoggerFactory.getLogger(HttpEndpoint.class);

    private final String endpoint;
    private final long connectTimeout;
    private final long readTimeout;
    private final int batchSize;
    private final int maxConcurrent;
    private final IAuthenticationProvider authenticationProvider;
    private final String contentType;
    private final IEventFormatter eventFormatter;
    private final List<Integer> validStatusCodes;
    private ExecutorService executor;
    HttpClient httpClient;
    
    private List<List<SinkRecord>> batches = new ArrayList<>();
    
    public HttpEndpoint(
            String endpoint,
            long connectTimeout, long readTimeout,
            int batchSize,
            IAuthenticationProvider authenticationProvider,
            String contentType,
            IEventFormatter eventFormatter,
            List<Integer> validStatusCodes,
            ExecutorService executor,
            int maxConcurrent) {
        this.endpoint = endpoint;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.batchSize = batchSize;
        this.authenticationProvider = authenticationProvider;
        this.contentType = contentType;
        this.eventFormatter = eventFormatter;
        this.validStatusCodes = validStatusCodes;
        this.executor = executor;
        this.maxConcurrent = maxConcurrent;
        this.httpClient = HttpClient.newBuilder()
                                    .connectTimeout(Duration.ofMillis(this.connectTimeout))
                                    .build();

    }
    
    
    public void write(Collection<SinkRecord> records) throws IOException, InterruptedException {
        List<SinkRecord> batch = new ArrayList<>();
        for (SinkRecord record : records) {
            batch.add(record);
            
            // We got a complete batch
            if (batch.size() >= this.batchSize) {
                this.batches.add(batch);
                batch = new ArrayList<>();
            }

            if (this.batches.size() >= this.maxConcurrent) {
                this.sendBatches();
            }
        }
        this.sendBatches();
    }

    private void sendBatches() throws IOException, InterruptedException {
        if (this.batches.isEmpty())
            return;
        List<Future<HttpResponse<String>>> futures = new ArrayList<>();
        for (List<SinkRecord> batch : this.batches) {
            if (batch.isEmpty())
                continue;
            futures.add(executor.submit(() -> sendBatch(batch)));
        }
        try {
            for (Future<HttpResponse<String>> future : futures) {
                future.get();         
            }
        }
        catch (Exception e) {
            throw new IOException("Error while sending batch requests", e);
        }
        finally {
            this.batches.clear();
        }
    }
    
    private HttpResponse<String> sendBatch(List<SinkRecord> batch) throws IOException, InterruptedException {        
        log.debug("Sending batch of {} events to {}", batch.size(), this.endpoint);
        
        HttpResponse<String> resp;
        
        try {
            resp = this.invoke(this.eventFormatter.formatEventBatch(batch));
        } catch(AuthException authEx) {
            throw new HttpResponseException(this.endpoint, "Authentication error: " + authEx.getMessage());
        }
        
        // Check status code
        if (resp != null) {
            for(int statusCode : this.validStatusCodes) {
                if (statusCode == resp.statusCode()) {
                    log.debug("Response from HTTP endpoint {}: {} (status code: {})", this.endpoint, resp.body(), resp.statusCode());
                    return resp;
                }
            }
        }
        
        throw new HttpResponseException(this.endpoint, "Invalid response from HTTP endpoint", resp);
    }
    
    private HttpResponse<String> invoke(byte[] data) throws AuthException, IOException, InterruptedException {               
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(this.endpoint))
                .timeout(Duration.ofMillis(this.readTimeout))
                .header("Content-Type", this.contentType)
                .POST(BodyPublishers.ofString(new String(data)))
                ;
        
        // Add HTTP authentication header(s)
        if (this.authenticationProvider.addAuthentication() != null)
            reqBuilder.headers(this.authenticationProvider.addAuthentication());
        
        return this.httpClient.send(reqBuilder.build(), BodyHandlers.ofString());
    }
    
}
