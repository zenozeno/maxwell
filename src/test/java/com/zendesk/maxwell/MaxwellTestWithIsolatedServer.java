package com.zendesk.maxwell;

import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.BeforeClass;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.filtering.InvalidFilterException;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.MysqlVersion;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.Logging;

public class MaxwellTestWithIsolatedServer extends TestWithNameLogging {
	protected static MysqlIsolatedServer server;

	@BeforeClass
	public static void setupTest() throws Exception {
		Logging.setupLogBridging();
		server = MaxwellTestSupport.setupServer();
	}

	@Before
	public void setupSchema() throws Exception {
		MaxwellTestSupport.setupSchema(server);
	}

	protected List<RowMap> getRowsForSQL(final Filter filter, final String[] input) throws Exception {
		return getRowsForSQL(filter, input, null);
	}

	protected List<RowMap> getRowsForSQL(final String[] input) throws Exception {
		return getRowsForSQL(null, input, null);
	}

	protected List<RowMap> getRowsForSQL(final Filter filter, final String[] input, final String[] before) throws Exception {
		return MaxwellTestSupport.getRowsWithReplicator(server, input, before, (config) -> {
			if (filter != null) {
				try {
					filter.addRule("include: test.*");
				} catch (final InvalidFilterException e) {
				}
			}

			config.filter = filter;
		});
	}

	protected List<RowMap> getRowsForSQLTransactional(final String[] input) throws Exception {
		final MaxwellTestSupportTXCallback cb = new MaxwellTestSupportTXCallback(input);
		return MaxwellTestSupport.getRowsWithReplicator(server, cb, null);
	}

	protected List<RowMap> getRowsForDDLTransaction(final String[] input, final Filter filter) throws Exception {
		final MaxwellTestSupportTXCallback cb = new MaxwellTestSupportTXCallback(input);
		return MaxwellTestSupport.getRowsWithReplicator(server, cb, (config) -> {
			config.outputConfig = new MaxwellOutputConfig();
			config.outputConfig.outputDDL = true;
			config.filter = filter;
		});
	}

	protected void runJSON(final String filename) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, null);
	}

	protected void runJSON(final String filename, final Consumer<MaxwellConfig> configLambda) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, configLambda);
	}

	protected void runJSON(final String filename, final MaxwellOutputConfig config) throws Exception {
		MaxwellTestJSON.runJSONTestFile(server, filename, null, config);
	}

	protected MaxwellContext buildContext() throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), null, null);
	}

	protected MaxwellContext buildContext(final Position p) throws Exception {
		return MaxwellTestSupport.buildContext(server.getPort(), p, null);
	}

	protected Filter excludeTable(final String name) throws InvalidFilterException {
		final Filter filter = new Filter("exclude: *." + name);
		return filter;
	}

	protected Filter excludeDb(final String name) throws InvalidFilterException {
		final Filter filter = new Filter("exclude: " + name + ".*");
		return filter;
	}

	protected void requireMinimumVersion(final MysqlVersion minimum) {
		// skips this test if running an older MYSQL version
		assumeTrue(server.getVersion().atLeast(minimum));
	}

	private class MaxwellTestSupportTXCallback extends MaxwellTestSupportCallback {
		private final String[] input;

		public MaxwellTestSupportTXCallback(final String[] input) {
			this.input = input;
		}

		@Override
		public void afterReplicatorStart(final MysqlIsolatedServer mysql) throws SQLException {
			final Connection c = mysql.getNewConnection();
			c.setAutoCommit(false);
			for (final String s : input) {
				c.createStatement().execute(s);
			}
			c.commit();
		}
	}

	static {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}
}
