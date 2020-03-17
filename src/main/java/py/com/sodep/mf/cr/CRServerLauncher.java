package py.com.sodep.mf.cr;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import py.com.sodep.mf.cr.webapi.WebApiClient;
import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;

@SpringBootApplication
public class CRServerLauncher {
	private static final Logger logger = Logger.getLogger(CRServerLauncher.class);
	
	private static String propertyFilePath = "conf/mf_cr.properties"; //path a las configuraciones
	
	public static final String PROP_MODE = "mf_cr.mode";
	public static final String PROP_LOG4J = "mf_cr.log4jFile";	//Ubicación del archivo xml que configura log4j
	public static final String PROP_BASEURL = "mf_cr.rest.baseURL";	//URL de captura
	public static final String PROP_REST_APP_ID = "mf_cr.rest.applicationId";	//Id de captura
	public static final String PROP_REST_USER = "mf_cr.rest.user";	//Usuario de cuenta captura
	public static final String PROP_REST_PASS = "mf_cr.rest.pass";	//Contrasena de cuenta captura
	
	public static final String PROP_DB_USER = "mf_cr.db.user";	//usuario DB h2
	public static final String PROP_DB_PASS = "mf_cr.db.pass";	//contrasena DB h2
	public static final String PROP_DB_FILEPATH = "mf_cr.db.filePath";	//path DB h2
	public static final String PROP_AUTHENTICATE_ON_STARTUP = "mf_cr.authenticateOnStartup";
	public static final String PROP_XML_FILE = "mf_cr.xml.filePath";	//path al xml que configura la conexion a DB exterior
	
	public static final String PROP_WEBADMIN_WAKE_ON_STARTUP = "mf_cr.webadmin.wakeOnStartup";
	public static final String PROP_WEBADMIN_PORT = "mf_cr.webadmin.port";
	public static final String PROP_WEBADMIN_USER = "mf_cr.webadmin.user";
	public static final String PROP_WEBADMIN_PASSWORD = "mf_cr.webadmin.password";
	
	private static OSSignalHandler osSignalhandler;
	
	// trust on all certificates
	private static boolean testingMode = true;
	
	private WebApiClient restClient;
	
	public static void main(String[] args) {
		SpringApplication.run(CRServerLauncher.class, args);
	}
	
	@Bean
    void service_Launcher() {
    	DOMConfigurator.configure("log4j.xml");
		restClient = new WebApiClient("https://captura-forms.com/mf/","matias.irala.aveiro@gmail.com", "luca es del milan");
		logger.info("Testing connection...");
		boolean success;
		try {
			success = restClient.login();
			if (success) {
				logger.info("Successfully logged in to ");
				logger.info("Login successful with user ");
				try {
					restClient.logout();
				} catch (WebApiException e) {
					logger.error("Problems on the first logout", e);
					System.exit(1);
				} catch (RestAuhtorizationException e) {
					logger.error("Problems on the first logout", e);
					System.exit(1);
				}
			} else {
				logger.error("Couldn't authenticate to the server");
				System.exit(1);
			}
		} catch (IOException e) {
			logger.error("Couldn't connect to MF server at ");
			logger.debug("Couldn't connect to MF server at ", e);
			System.exit(1);
		}
	}

}
