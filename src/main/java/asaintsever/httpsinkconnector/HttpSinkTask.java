package asaintsever.httpsinkconnector;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import asaintsever.httpsinkconnector.config.HttpSinkConnectorConfig;
import asaintsever.httpsinkconnector.event.formatter.IEventFormatter;
import asaintsever.httpsinkconnector.http.HttpEndpoint;
import asaintsever.httpsinkconnector.http.authentication.IAuthenticationProvider;
import asaintsever.httpsinkconnector.utils.HttpSinkConnectorInfo;

public class HttpSinkTask extends SinkTask {
    
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
    
    private HttpSinkConnectorConfig config;
    private HttpEndpoint endpoint;
    private int retryIndex;
    private ExecutorService executor;
    
    
    public HttpSinkTask() {        
    }

    @Override
    public String version() {
        return HttpSinkConnectorInfo.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting CCS Event Publisher HTTP Sink Task");
        this.config = new HttpSinkConnectorConfig(props);
        this.executor = Executors.newFixedThreadPool(config.getHttpMaxConcurrent() > 1 ? 25 : 1);

        IAuthenticationProvider authProv = config.getHttpReqAuthProvider();
        authProv.configure(config.originalsWithPrefix(HttpSinkConnectorConfig.HTTP_REQ_AUTHENTICATION_PROVIDER_CLASS_PARAM_PREFIX));
        
        IEventFormatter eventFormatter = config.getEventFormatter();
        eventFormatter.configure(config.originalsWithPrefix(HttpSinkConnectorConfig.EVENT_FORMATTER_CLASS_PARAM_PREFIX));
        
        this.endpoint = new HttpEndpoint(
                this.config.getHttpEndpoint(), 
                this.config.getHttpConnectTimeout(),
                this.config.getHttpReadTimeout(),
                this.config.getEventMaxBatchSize(), 
                authProv,
                this.config.getHttpReqContentType(), 
                eventFormatter,
                this.config.getHttpRespValidStatusCodes(),
                this.executor,
                this.config.getHttpMaxConcurrent());
        
        this.retryIndex = 0;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        
        if(this.config.getHttpLogReceivedRecordsEnabled())
            log.info("Received {} records", records.size());
        
        try {
            this.endpoint.write(records);
        } catch (Exception e) {
            log.warn("Write of {} records failed, retry={}/{}", records.size(), this.retryIndex, this.config.getHttpReqRetryMaxAttempts(), e);
            
            if (this.retryIndex == this.config.getHttpReqRetryMaxAttempts()) {
                if (this.config.getBehaviorOnError().equals(HttpSinkConnectorConfig.BEHAVIOR_ON_ERROR_FAIL)) {
                    throw new ConnectException(e);
                }
                
            } else {
                // This retry behavior is not ideal when using max.concurrent > 1
                // It would be better to retry only the failed request rather then the entire concurrent batch
                this.context.timeout(this.config.getHttpReqRetryExpBackoffBaseIntervalMs() * (this.retryIndex == 0 ? 1 : (long) (Math.pow(this.config.getHttpReqRetryExpBackoffMultiplier(), this.retryIndex))));
                this.retryIndex++;
                throw new RetriableException(e);
            }
        }
        
        this.retryIndex = 0;
    }

    @Override
    public void stop() {
        log.info("Stopping CCS Event Publisher HTTP Sink Task");
        this.config = null;
        this.endpoint = null;
        this.executor.shutdown();
    }

}
