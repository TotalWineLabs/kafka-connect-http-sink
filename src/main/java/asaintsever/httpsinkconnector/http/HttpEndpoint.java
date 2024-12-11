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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import asaintsever.httpsinkconnector.event.formatter.IEventFormatter;
import asaintsever.httpsinkconnector.http.authentication.AuthException;
import asaintsever.httpsinkconnector.http.authentication.IAuthenticationProvider;

public class HttpEndpoint {

    private static final Logger log = LoggerFactory.getLogger(HttpEndpoint.class);

    private final Boolean logReceivedRecords;
    private final String endpoint;
    private final long connectTimeout;
    private final long readTimeout;
    private final int batchSize;
    private final int maxConcurrent;
    private final int retryMaxAttempts;
    private final long retryExpBackoffIntervalMs;
    private final double retryExpBackoffMultipler;
    private final long retryMaxDurationMs;
    private final IAuthenticationProvider authenticationProvider;
    private final String contentType;
    private final IEventFormatter eventFormatter;
    private final List<Integer> validStatusCodes;
    private ExecutorService executor;
    HttpClient httpClient;

    private List<List<SinkRecord>> batches = new ArrayList<>();

    public HttpEndpoint(
            Boolean logReceivedRecords,
            String endpoint,
            long connectTimeout, long readTimeout,
            int batchSize,
            IAuthenticationProvider authenticationProvider,
            String contentType,
            IEventFormatter eventFormatter,
            List<Integer> validStatusCodes,
            int maxConcurrent,
            int retryMaxAttempts,
            long retryExpBackoffIntervalMs,
            double retryExpBackoffMultipler,
            long retryMaxDurationMs) {
        this.logReceivedRecords = logReceivedRecords;
        this.endpoint = endpoint;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.batchSize = batchSize;
        this.authenticationProvider = authenticationProvider;
        this.contentType = contentType;
        this.eventFormatter = eventFormatter;
        this.validStatusCodes = validStatusCodes;
        this.maxConcurrent = maxConcurrent;
        this.retryMaxAttempts = retryMaxAttempts;
        this.retryExpBackoffIntervalMs = retryExpBackoffIntervalMs;
        this.retryExpBackoffMultipler = retryExpBackoffMultipler;
        this.retryMaxDurationMs = retryMaxDurationMs;
        this.executor = Executors.newFixedThreadPool(maxConcurrent > 1 ? 25 : 1);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.connectTimeout))
                .build();
    }

    public void write(Collection<SinkRecord> records) throws IOException, InterruptedException {

        int batchSize = this.batchSize;
        int numberOfBatches = (int) Math.ceil((double) records.size() / batchSize);

        if (this.logReceivedRecords)
            log.info("Received {} records split into {} batches of size {}", records.size(), numberOfBatches,
                    batchSize);

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

        // Add any remaining records
        if (!batch.isEmpty()) {
            this.batches.add(batch);
        }

        // Ensure remaining batches are sent
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
        } catch (Exception e) {
            throw new IOException("Error while sending batch requests", e);
        } finally {
            this.batches.clear();
        }
    }

    private HttpResponse<String> sendBatch(List<SinkRecord> batch) throws IOException, InterruptedException {
        log.debug("Sending batch of {} events to {}", batch.size(), this.endpoint);

        HttpResponse<String> resp = null;
        int retryIndex = 0;
        long startTime = System.currentTimeMillis();

        do {
            try {
                try {
                    resp = this.invoke(this.eventFormatter.formatEventBatch(batch));
                } catch (AuthException authEx) {
                    throw new HttpResponseException(this.endpoint, "Authentication error: " + authEx.getMessage());
                }
                // Check status code
                if (resp != null) {
                    for (int statusCode : this.validStatusCodes) {
                        if (statusCode == resp.statusCode()) {
                            log.debug("Response from HTTP endpoint {}: {} (status code: {})", this.endpoint,
                                    resp.body(), resp.statusCode());
                            return resp;
                        }
                    }
                }
                throw new HttpResponseException(this.endpoint, "Invalid response from HTTP endpoint", resp);

            } catch (Exception e) {
                log.info("Write of {} records failed, retry={}/{}", batch.size(), retryIndex, this.retryMaxAttempts, e);
                // Calculate timeout
                long timeout = this.retryExpBackoffIntervalMs *
                        (retryIndex == 0 ? 1
                                : (long) Math.min(Math.pow(this.retryExpBackoffMultipler, retryIndex), Integer.MAX_VALUE));

                // Sleep for the calculated timeout
                try {
                    log.info("Retrying after {} ms...", timeout);
                    Thread.sleep(timeout);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                    throw new IOException("Thread interrupted during retry sleep", ie);
                }

                retryIndex++;
            }

        } while (retryIndex < this.retryMaxAttempts
                && (System.currentTimeMillis() - startTime) < this.retryMaxDurationMs);

        throw new HttpResponseException(this.endpoint, "Invalid response from HTTP endpoint", resp);
    }

    private HttpResponse<String> invoke(byte[] data) throws AuthException, IOException, InterruptedException {
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(this.endpoint))
                .timeout(Duration.ofMillis(this.readTimeout))
                .header("Content-Type", this.contentType)
                .POST(BodyPublishers.ofString(new String(data)));

        // Add HTTP authentication header(s)
        if (this.authenticationProvider.addAuthentication() != null)
            reqBuilder.headers(this.authenticationProvider.addAuthentication());

        return this.httpClient.send(reqBuilder.build(), BodyHandlers.ofString());
    }

    public void close() {
        this.httpClient.close();
        this.executor.shutdown();
    }
}
