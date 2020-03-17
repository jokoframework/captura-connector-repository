package py.com.sodep.mf.cr.conf;

/**
 * This is a temporal problem and it might get fixed with an external action.
 * For example, the DB is not accesible, or the remote table doesn't exists
 * 
 * @author danicricco
 * 
 */
public class TemporalConfigurationError extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1L;

	public TemporalConfigurationError(String msg) {
		super(msg);
	}

	public TemporalConfigurationError(String msg, Throwable e) {
		super(msg, e);
	}
}
