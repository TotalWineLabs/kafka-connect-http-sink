package asaintsever.httpsinkconnector.http.authentication;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

public class GCPAuthenticationProvider implements IAuthenticationProvider {

    private GoogleCredentials credentials;
    private String audience;

    @Override
    public ConfigDef configDef() {
        return GCPAuthenticationProviderConfig.configDef();
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        GCPAuthenticationProviderConfig config = new GCPAuthenticationProviderConfig(configs);
        try {
            this.credentials = ((ServiceAccountCredentials) config.getCredentials())
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
            this.audience = config.getAudience();
        } catch (IOException e) {
            this.credentials = null;
            e.printStackTrace();
        }
    }
    
    @Override
    public String[] addAuthentication() throws AuthException {
        if (this.credentials == null) {
            throw new AuthException("No credentials available");
        }

        try {
            // Obtain an ID Token for the Cloud Function URL
            IdTokenCredentials tokenCredentials = IdTokenCredentials.newBuilder()
                    .setIdTokenProvider((ServiceAccountCredentials) this.credentials)
                    .setTargetAudience(this.audience)
                    .build();
            tokenCredentials.refreshIfExpired();
            IdToken idToken = tokenCredentials.getIdToken();
            String idTokenValue = idToken.getTokenValue();
            return new String[] {"Authorization", "Bearer " + idTokenValue};

        } catch (IOException e) {
            e.printStackTrace();
            throw new AuthException("Failed to obtain token");
        }
        
    }
}
