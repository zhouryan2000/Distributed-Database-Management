package testing;

import java.io.IOException;

import app_kvECS.ECSClient;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	public static KVServer server;

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			server = new KVServer(50000, 10, "FIFO");
			new Thread(server).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		// clientSuite.addTestSuite(ConnectionTest.class);
		// clientSuite.addTestSuite(InteractionTest.class);
		// clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(PerformanceTest.class);
		clientSuite.addTestSuite(M4AdditionalTest.class);
//		clientSuite.addTestSuite(M4PerformanceTest.class);
		return clientSuite;
	}
	
}
