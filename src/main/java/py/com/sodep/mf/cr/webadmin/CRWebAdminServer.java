package py.com.sodep.mf.cr.webadmin;

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;

import py.com.sodep.mf.cr.conf.ConnectorDefinition;

public class CRWebAdminServer {
	private final String username;
	private final String password;

	private Server server;
	private CRWebAdminServlet servlet;

	public CRWebAdminServer(int port, String username, String password, ConnectorDefinition definition) {
		super();
		this.username = username;
		this.password = password;
		this.server = new Server(port);
		if(definition!=null)
			this.servlet = new CRWebAdminServlet(definition);
	}

	public void start() throws Exception {
		SecurityHandler securityHandler = basicAuth(username, password, "Private site");

		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setSecurityHandler(securityHandler);
		context.setContextPath("/");
		if(this.servlet==null)
			this.servlet = new CRWebAdminServlet(null);
		context.addServlet(new ServletHolder(this.servlet), "/*");

		ContextHandler assetsContext = new ContextHandler();
		assetsContext.setContextPath("/static");
		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setResourceBase("web/assets");
		assetsContext.setHandler(resourceHandler);

		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[] { assetsContext, context });
		server.setHandler(handlers);

		server.start();
	}

	public void stop() throws Exception {
		this.server.stop();
	}

	private static final SecurityHandler basicAuth(String username, String password, String realm) {
		HashLoginService l = new HashLoginService();
		UserStore userStore = new UserStore();
		userStore.addUser(username, new Password(password), new String[] { "user"});
		//l.putUser(username, Credential.getCredential(password), new String[] { "user" });
		l.setUserStore(userStore);
		l.setName(realm);

		Constraint constraint = new Constraint();
		constraint.setName(Constraint.__BASIC_AUTH);
		constraint.setRoles(new String[] { "user" });
		constraint.setAuthenticate(true);

		ConstraintMapping cm = new ConstraintMapping();
		cm.setConstraint(constraint);
		cm.setPathSpec("/*");

		ConstraintSecurityHandler csh = new ConstraintSecurityHandler();
		csh.setAuthenticator(new BasicAuthenticator());
		csh.setRealmName("myrealm");
		csh.addConstraintMapping(cm);
		csh.setLoginService(l);

		return csh;
	}
}
