package asaintsever.httpsinkconnector;

import com.google.common.collect.ImmutableMap;

import asaintsever.httpsinkconnector.config.HttpSinkConnectorConfig;
import asaintsever.httpsinkconnector.http.authentication.NoAuthenticationProvider;
import asaintsever.httpsinkconnector.http.HttpEndpoint;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

public class BatchTest {

    public HttpSinkConnectorConfig config;
    public HttpEndpoint endpoint;

    SinkRecord map(Map value) {
        return new SinkRecord(
            "asdf",
            1,
            null,
            null,
            null,
            value,
            1234L);
      }

      SinkRecord string(String value) {
        return new SinkRecord(
            "asdf",
            1,
            null,
            null,
            null,
            value,
            1234L);
      }      

    @BeforeEach
    public void setUp() {
        Map<String, String> props = Map.of(
            "http.log.received.records.enabled", "true",
            "http.endpoint", "https://api-generator.retool.com/sOZhzC/data", // TODO: mock a server
            "http.max.concurrent", "1",
            "http.resp.valid.status.codes", "200,201,202,204",
            "http.request.retry.maxattempts", "2"
        );
        this.config = new HttpSinkConnectorConfig(props);
        this.endpoint = new HttpEndpoint(
            this.config.getHttpLogReceivedRecordsEnabledConfig(),
            this.config.getHttpEndpoint(),
            this.config.getHttpConnectTimeout(),
            this.config.getHttpReadTimeout(),
            this.config.getEventMaxBatchSize(),
            this.config.getHttpReqAuthProvider(),
            this.config.getHttpReqContentType(),
            this.config.getEventFormatter(),
            this.config.getHttpRespValidStatusCodes(),
            this.config.getHttpMaxConcurrent(),
            this.config.getHttpReqRetryMaxAttempts(),
            this.config.getHttpReqRetryExpBackoffBaseIntervalMs(),
            this.config.getHttpReqRetryExpBackoffMultiplier(),
            this.config.getHttpReqRetryMaxDurationMs());       
    }

    @Test
    public void testBatch() {

        SinkRecord record1 = string("{ \"id\": \"John Do1\" }");
        SinkRecord record2 = string("{ \"id\": \"John Do2\" }");
        SinkRecord record3 = string("{ \"id\": \"John Do3\" }");
        List<SinkRecord> records = List.of(record1, record2, record3);

        try {
            this.endpoint.write(records);
        } catch (Exception e) {
            assertNotNull(null, "Exception should not be thrown");
        }
        
      }
}
