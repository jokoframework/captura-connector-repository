package py.com.sodep.mf.cr.conf;

/**
 * This is an error that only the user can fix. It might be a problem on the
 * configuration file. There is no sense on keeping the connector repository
 * running if this kind of error happened
 * 
 * @author danicricco
 * 
 */
public class UnrecoverableConfigurationError extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnrecoverableConfigurationError(String msg) {
		super(msg);
	}

}
