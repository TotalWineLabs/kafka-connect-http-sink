package asaintsever.httpsinkconnector;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
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

        IAuthenticationProvider authProv = config.getHttpReqAuthProvider();
        authProv.configure(config
                .originalsWithPrefix(HttpSinkConnectorConfig.HTTP_REQ_AUTHENTICATION_PROVIDER_CLASS_PARAM_PREFIX));

        IEventFormatter eventFormatter = config.getEventFormatter();
        eventFormatter
                .configure(config.originalsWithPrefix(HttpSinkConnectorConfig.EVENT_FORMATTER_CLASS_PARAM_PREFIX));

        this.endpoint = new HttpEndpoint(
                this.config.getHttpLogReceivedRecordsEnabledConfig(),
                this.config.getHttpEndpoint(),
                this.config.getHttpConnectTimeout(),
                this.config.getHttpReadTimeout(),
                this.config.getEventMaxBatchSize(),
                authProv,
                this.config.getHttpReqContentType(),
                eventFormatter,
                this.config.getHttpRespValidStatusCodes(),
                this.config.getHttpMaxConcurrent(),
                this.config.getHttpReqRetryMaxAttempts(),
                this.config.getHttpReqRetryExpBackoffBaseIntervalMs(),
                this.config.getHttpReqRetryExpBackoffMultiplier(),
                this.config.getHttpReqRetryMaxDurationMs());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            this.endpoint.write(records);
        } catch (Exception e) {
            if (this.config.getBehaviorOnError().equals(HttpSinkConnectorConfig.BEHAVIOR_ON_ERROR_FAIL)) {
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Stopping CCS Event Publisher HTTP Sink Task");
        this.config = null;
        this.endpoint.close();
    }

}
