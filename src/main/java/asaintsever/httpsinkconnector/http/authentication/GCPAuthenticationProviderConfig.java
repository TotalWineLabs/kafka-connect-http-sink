package asaintsever.httpsinkconnector.http.authentication;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

public class GCPAuthenticationProviderConfig extends AbstractConfig {
    
    public static final String AUTH_GROUP ="GCPAuthentication";
    
    public static final String CREDENTIALS_PATH_CONFIG = "credentials.path";
    public static final String CREDENTIALS_PATH_DISPLAYNAME = "Service Account Path";
    public static final String CREDENTIALS_PATH_DOC = "Path to service account key file";

    public static final String AUDIENCE_CONFIG = "audience";
    public static final String AUDIENCE_DISPLAYNAME = "Credentials Audience";
    public static final String AUDIENCE_DOC = "Credentials Audience";    
    
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(CREDENTIALS_PATH_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.MEDIUM, CREDENTIALS_PATH_DOC, AUTH_GROUP, 0, Width.MEDIUM, CREDENTIALS_PATH_DISPLAYNAME)
            .define(AUDIENCE_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.LOW, AUDIENCE_DOC, AUTH_GROUP, 0, Width.MEDIUM, AUDIENCE_DISPLAYNAME)
            ;
    }
    
    public GCPAuthenticationProviderConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }
    
    public Credentials getCredentials() throws IOException {
        FileInputStream inputStream = new FileInputStream(getString(CREDENTIALS_PATH_CONFIG));
        return ServiceAccountCredentials.fromStream(inputStream);
    }

    public String getAudience() {
        return getString(AUDIENCE_CONFIG);
    }
}
