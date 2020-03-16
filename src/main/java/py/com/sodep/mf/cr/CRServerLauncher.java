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
