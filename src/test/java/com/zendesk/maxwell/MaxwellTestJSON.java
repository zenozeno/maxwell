package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowEncrypt;
import com.zendesk.maxwell.row.RowMap;

public class MaxwellTestJSON {
	/* methods around running JSON test files */
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {
	};
	static final String JSON_PATTERN = "^\\s*\\->\\s*\\{.*";

	public static Map<String, Object> parseJSON(final String json) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		return mapper.readValue(json, MAP_STRING_OBJECT_REF);
	}

	public static Map<String, Object> parseEncryptedJSON(final Map<String, Object> json, final String secretKey) throws Exception {
		final Map<String, String> encrypted = (Map)json.get("encrypted");
		if (encrypted == null) {
			return null;
		}
		final String init_vector = encrypted.get("iv");
		final String plaintext = RowEncrypt.decrypt(encrypted.get("bytes").toString(), secretKey, init_vector);
		return parseJSON(plaintext);
	}

	public static void assertJSON(final List<Map<String, Object>> jsonOutput, final List<Map<String, Object>> jsonAsserts) {
		final ArrayList<Map<String, Object>> missing = new ArrayList<>();

		for (final Map m : jsonAsserts) {
			if (!jsonOutput.contains(m))
				missing.add(m);
		}

		if (missing.size() > 0) {
			final String msg = "Did not find:\n" +
					StringUtils.join(missing.iterator(), "\n") +
					"\n\n in:\n" +
					StringUtils.join(jsonOutput.iterator(), "\n");
			assertThat(msg, false, is(true));
		}
	}

	private static void runJSONTest(final MysqlIsolatedServer server, final List<String> sql, final List<Map<String, Object>> expectedJSON,
			final Consumer<MaxwellConfig> configLambda, final Optional<MaxwellOutputConfig> config) throws Exception {
		final List<Map<String, Object>> eventJSON = new ArrayList<>();

		final MaxwellConfig captureConfig = new MaxwellConfig();
		if (configLambda != null)
			configLambda.accept(captureConfig);

		final MaxwellOutputConfig outputConfig = config.orElse(captureConfig.outputConfig);

		final List<RowMap> rows = MaxwellTestSupport.getRowsWithReplicator(server, sql.toArray(new String[sql.size()]), null, configLambda);

		for (final RowMap r : rows) {
			final String s;
			if (outputConfig == null) {
				s = r.toJSON();
			} else {
				s = r.toJSON(outputConfig);
			}

			final Map<String, Object> outputMap = parseJSON(s);

			outputMap.remove("ts");
			outputMap.remove("xid");
			outputMap.remove("xoffset");
			outputMap.remove("commit");

			eventJSON.add(outputMap);
		}
		assertJSON(eventJSON, expectedJSON);

	}

	public static SQLAndJSON parseJSONTestFile(final String fname) throws Exception {
		final File file = new File(fname);
		final SQLAndJSON ret = new SQLAndJSON();
		final BufferedReader reader = new BufferedReader(new FileReader(file));
		final ObjectMapper mapper = new ObjectMapper();

		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

		String buffer = null;
		boolean bufferIsJSON = false;
		while (reader.ready()) {
			String line = reader.readLine();

			if (line.matches("^\\s*$")) { // skip blanks
				continue;
			}

			if (buffer != null) {
				if (line.matches("^\\s+.*$") && !line.matches(JSON_PATTERN)) { // leading whitespace -- continuation of previous line
					buffer = buffer + " " + line.trim();
				} else {
					if (bufferIsJSON) {
						ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MAP_STRING_OBJECT_REF));
					} else {
						ret.inputSQL.add(buffer);
					}
					buffer = null;
				}
			}

			if (buffer == null) {
				if (line.matches(JSON_PATTERN)) {
					line = line.replaceAll("^\\s*\\->\\s*", "");
					bufferIsJSON = true;
				} else {
					bufferIsJSON = false;
				}
				buffer = line;
			}
		}

		if (buffer != null) {
			if (bufferIsJSON) {
				ret.jsonAsserts.add(mapper.<Map<String, Object>>readValue(buffer, MAP_STRING_OBJECT_REF));
			} else {
				ret.inputSQL.add(buffer);
			}
		}

		reader.close();
		return ret;
	}

	protected static void runJSONTestFile(final MysqlIsolatedServer server, final String fname, final Consumer<MaxwellConfig> configLambda) throws Exception {
		final String dir = MaxwellTestSupport.getSQLDir();
		final SQLAndJSON testResources = parseJSONTestFile(new File(dir, fname).toString());

		runJSONTest(server, testResources.inputSQL, testResources.jsonAsserts, configLambda, Optional.empty());
	}

	protected static void runJSONTestFile(
			final MysqlIsolatedServer server, final String fname, final Consumer<MaxwellConfig> configLambda, final MaxwellOutputConfig config)
			throws Exception {
		final String dir = MaxwellTestSupport.getSQLDir();
		final SQLAndJSON testResources = parseJSONTestFile(new File(dir, fname).toString());

		runJSONTest(server, testResources.inputSQL, testResources.jsonAsserts, configLambda, Optional.of(config));
	}

	public static class SQLAndJSON {
		public ArrayList<Map<String, Object>> jsonAsserts;
		public ArrayList<String> inputSQL;

		protected SQLAndJSON() {
			jsonAsserts = new ArrayList<>();
			inputSQL = new ArrayList<>();
		}
	}
}
