package com.zendesk.maxwell.row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.zendesk.maxwell.errors.ProtectedAttributeNameException;
import com.zendesk.maxwell.producer.DataOutput;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;

public class RowMap implements Serializable {

	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);
	private static final JsonFactory jsonFactory = new JsonFactory();
	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>() {
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};
	private static final ThreadLocal<JsonGenerator> jsonGeneratorThreadLocal =
			new ThreadLocal<JsonGenerator>() {
				@Override
				protected JsonGenerator initialValue() {
					JsonGenerator g = null;
					try {
						g = jsonFactory.createGenerator(byteArrayThreadLocal.get());
					} catch (final IOException e) {
						LOGGER.error("error initializing jsonGenerator", e);
						return null;
					}
					g.setRootValueSeparator(null);
					return g;
				}
			};
	private static final ThreadLocal<DataJsonGenerator> plaintextDataGeneratorThreadLocal =
			new ThreadLocal<DataJsonGenerator>() {
				@Override
				protected DataJsonGenerator initialValue() {
					return new PlaintextJsonGenerator(jsonGeneratorThreadLocal.get());
				}
			};
	private static final ThreadLocal<EncryptingJsonGenerator> encryptingJsonGeneratorThreadLocal =
			new ThreadLocal<EncryptingJsonGenerator>() {
				@Override
				protected EncryptingJsonGenerator initialValue() {
					try {
						return new EncryptingJsonGenerator(jsonGeneratorThreadLocal.get(), jsonFactory);
					} catch (final IOException e) {
						LOGGER.error("error initializing EncryptingJsonGenerator", e);
						return null;
					}
				}
			};
	private final String rowQuery;
	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestampMillis;
	private final Long timestampSeconds;
	private final Position position;
	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> oldData;
	private final LinkedHashMap<String, Object> extraAttributes;
	private final List<String> pkColumns;
	private final Position nextPosition;
	protected boolean suppressed;
	private String kafkaTopic;
	private Long xid;
	private Long xoffset;
	private boolean txCommit;
	private Long serverId;
	private Long threadId;
	private long approximateSize;

	public RowMap(final String type, final String database, final String table, final Long timestampMillis, final List<String> pkColumns,
			final Position position, final Position nextPosition, final String rowQuery) {
		this.rowQuery = rowQuery;
		rowType = type;
		this.database = database;
		this.table = table;
		this.timestampMillis = timestampMillis;
		timestampSeconds = timestampMillis / 1000;
		data = new LinkedHashMap<>();
		oldData = new LinkedHashMap<>();
		extraAttributes = new LinkedHashMap<>();
		this.position = position;
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
		suppressed = false;
		approximateSize = 100L; // more or less 100 bytes of overhead
	}

	public RowMap(final String type, final String database, final String table, final Long timestampMillis, final List<String> pkColumns,
			final Position nextPosition, final String rowQuery) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, nextPosition, rowQuery);
	}

	public RowMap(final String type, final String database, final String table, final Long timestampMillis, final List<String> pkColumns,
			final Position nextPosition) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, null);
	}

	private static JsonGenerator resetJsonGenerator() {
		byteArrayThreadLocal.get().reset();
		return jsonGeneratorThreadLocal.get();
	}

	//Do we want to encrypt this part?
	public String pkToJson(final KeyFormat keyFormat) throws IOException {
		if (keyFormat == KeyFormat.HASH)
			return pkToJsonHash();
		else
			return pkToJsonArray();
	}

	private String pkToJsonHash() throws IOException {
		final JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);

		if (pkColumns.isEmpty()) {
			g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
		} else {
			for (final String pk : pkColumns) {
				Object pkValue = null;
				if (data.containsKey(pk))
					pkValue = data.get(pk);

				writeValueToJSON(g, true, "pk." + pk.toLowerCase(), pkValue);
			}
		}

		g.writeEndObject(); // end of 'data: { }'
		g.flush();
		return jsonFromStream();
	}

	private String pkToJsonArray() throws IOException {
		final JsonGenerator g = resetJsonGenerator();

		g.writeStartArray();
		g.writeString(database);
		g.writeString(table);

		g.writeStartArray();
		for (final String pk : pkColumns) {
			Object pkValue = null;
			if (data.containsKey(pk))
				pkValue = data.get(pk);

			g.writeStartObject();
			writeValueToJSON(g, true, pk.toLowerCase(), pkValue);
			g.writeEndObject();
		}
		g.writeEndArray();
		g.writeEndArray();
		g.flush();
		return jsonFromStream();
	}

	public String pkAsConcatString() {
		if (pkColumns.isEmpty()) {
			return database + table;
		}
		final String keys = buildPartitionKey(pkColumns);

		if (keys.length() == 0)
			return "None";
		return keys;
	}

	public String buildPartitionKey(final List<String> partitionColumns) {
		final StringBuilder partitionKey = new StringBuilder();
		for (final String pc : partitionColumns) {
			Object pcValue = null;
			if (data.containsKey(pc))
				pcValue = data.get(pc);
			if (pcValue != null)
				partitionKey.append(pcValue.toString());
		}

		return partitionKey.toString();
	}

	private void writeMapToJSON(
			final String jsonMapName,
			final LinkedHashMap<String, Object> data,
			final JsonGenerator g,
			final boolean includeNullField
	) throws IOException {
		if (StringUtils.isNotBlank(jsonMapName)) {
			g.writeObjectFieldStart(jsonMapName);
		}
		for (final String key : data.keySet()) {
			final Object value = data.get(key);

			writeValueToJSON(g, includeNullField, key, value);
		}
		if (StringUtils.isNotBlank(jsonMapName)) {
			g.writeEndObject(); // end of 'jsonMapName: { }'
		}
	}

	private void writeValueToJSON(final JsonGenerator g, final boolean includeNullField, final String key, final Object value) throws IOException {
		if (value == null && !includeNullField)
			return;

		if (value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
			final List stringList = (List)value;

			g.writeArrayFieldStart(key);
			for (final Object s : stringList) {
				g.writeObject(s);
			}
			g.writeEndArray();
		} else if (value instanceof RawJSONString) {
			// JSON column type, using binlog-connector's serializers.
			g.writeFieldName(key);
			g.writeRawValue(((RawJSONString)value).json);
		} else {
			g.writeObjectField(key, value);
		}
	}

	public String toJSON() throws Exception {
		return toJSON(new MaxwellOutputConfig());
	}

	public String toJSON(final MaxwellOutputConfig outputConfig) throws Exception {
		final JsonGenerator g = resetJsonGenerator();
		final Boolean isDataOutputContent = DataOutput.CONTENT.equals(outputConfig.outputDataOnly);

		g.writeStartObject(); // start of row {

		if (DataOutput.FALSE.equals(outputConfig.outputDataOnly)) {
			generateMetadata(g, outputConfig);
		}

		if (!isDataOutputContent && outputConfig.excludeColumns.size() > 0) {
			// NOTE: to avoid concurrent modification.
			final Set<String> keys = new HashSet<>();
			keys.addAll(data.keySet());
			keys.addAll(oldData.keySet());

			for (final Pattern p : outputConfig.excludeColumns) {
				for (final String key : keys) {
					if (p.matcher(key).matches()) {
						data.remove(key);
						oldData.remove(key);
					}
				}
			}
		}

		EncryptionContext encryptionContext = null;
		if (outputConfig.encryptionEnabled()) {
			encryptionContext = EncryptionContext.create(outputConfig.secretKey);
		}

		final DataJsonGenerator dataWriter = outputConfig.encryptionMode == EncryptionMode.ENCRYPT_DATA
				? encryptingJsonGeneratorThreadLocal.get()
				: plaintextDataGeneratorThreadLocal.get();

		final JsonGenerator dataGenerator = dataWriter.begin();
		writeMapToJSON(!isDataOutputContent ? FieldNames.DATA : null, data, dataGenerator, outputConfig.includesNulls);
		if (!oldData.isEmpty()) {
			writeMapToJSON(!isDataOutputContent ? FieldNames.OLD : null, oldData, dataGenerator, outputConfig.includesNulls);
		}
		dataWriter.end(encryptionContext);

		g.writeEndObject(); // end of row
		g.flush();

		if (outputConfig.encryptionMode == EncryptionMode.ENCRYPT_ALL) {
			final String plaintext = jsonFromStream();
			encryptingJsonGeneratorThreadLocal.get().writeEncryptedObject(plaintext, encryptionContext);
			g.flush();
		}
		return jsonFromStream();
	}

	private void generateMetadata(final JsonGenerator g, final MaxwellOutputConfig outputConfig) throws IOException {
		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);

		if (outputConfig.includesRowQuery && rowQuery != null) {
			g.writeStringField(FieldNames.QUERY, rowQuery);
		}

		g.writeStringField(FieldNames.TYPE, rowType);
		g.writeNumberField(FieldNames.TIMESTAMP, timestampSeconds);

		if (outputConfig.includesCommitInfo) {
			if (xid != null)
				g.writeNumberField(FieldNames.TRANSACTION_ID, xid);

			if (outputConfig.includesXOffset && xoffset != null && !txCommit)
				g.writeNumberField(FieldNames.TRANSACTION_OFFSET, xoffset);

			if (txCommit)
				g.writeBooleanField(FieldNames.COMMIT, true);
		}

		final BinlogPosition binlogPosition = position.getBinlogPosition();
		if (outputConfig.includesBinlogPosition)
			g.writeStringField(FieldNames.POSITION, binlogPosition.getFile() + ":" + binlogPosition.getOffset());

		if (outputConfig.includesGtidPosition)
			g.writeStringField(FieldNames.GTID, binlogPosition.getGtid());

		if (outputConfig.includesServerId && serverId != null) {
			g.writeNumberField(FieldNames.SERVER_ID, serverId);
		}

		if (outputConfig.includesThreadId && threadId != null) {
			g.writeNumberField(FieldNames.THREAD_ID, threadId);
		}

		for (final Map.Entry<String, Object> entry : extraAttributes.entrySet()) {
			g.writeObjectField(entry.getKey(), entry.getValue());
		}
	}

	private String jsonFromStream() {
		final ByteArrayOutputStream b = byteArrayThreadLocal.get();
		final String s = b.toString();
		b.reset();
		return s;
	}

	public Object getData(final String key) {
		return data.get(key);
	}

	public Object getExtraAttribute(final String key) {
		return extraAttributes.get(key);
	}

	public long getApproximateSize() {
		return approximateSize;
	}

	private long approximateKVSize(final String key, final Object value) {
		long length = 0;
		length += 40; // overhead.  Whynot.
		length += key.length() * 2;

		if (value instanceof String) {
			length += ((String)value).length() * 2;
		} else {
			length += 64;
		}

		return length;
	}

	public void putData(final String key, final Object value) {
		data.put(key, value);

		approximateSize += approximateKVSize(key, value);
	}

	public void putExtraAttribute(final String key, final Object value) {
		if (FieldNames.isProtected(key)) {
			throw new ProtectedAttributeNameException("Extra attribute key name '" + key + "' is " +
					"a protected name. Must not be any of: " +
					String.join(", ", FieldNames.getFieldnames()));
		}
		extraAttributes.put(key, value);

		approximateSize += approximateKVSize(key, value);
	}

	public Object getOldData(final String key) {
		return oldData.get(key);
	}

	public void putOldData(final String key, final Object value) {
		oldData.put(key, value);

		approximateSize += approximateKVSize(key, value);
	}

	public Position getNextPosition() {
		return nextPosition;
	}

	public Position getPosition() {
		return position;
	}

	public Long getXid() {
		return xid;
	}

	public void setXid(final Long xid) {
		this.xid = xid;
	}

	public Long getXoffset() {
		return xoffset;
	}

	public void setXoffset(final Long xoffset) {
		this.xoffset = xoffset;
	}

	public void setTXCommit() {
		txCommit = true;
	}

	public boolean isTXCommit() {
		return txCommit;
	}

	public Long getServerId() {
		return serverId;
	}

	public void setServerId(final Long serverId) {
		this.serverId = serverId;
	}

	public Long getThreadId() {
		return threadId;
	}

	public void setThreadId(final Long threadId) {
		this.threadId = threadId;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public Long getTimestamp() {
		return timestampSeconds;
	}

	public Long getTimestampMillis() {
		return timestampMillis;
	}

	public boolean hasData(final String name) {
		return data.containsKey(name);
	}

	public String getRowQuery() {
		return rowQuery;
	}

	public String getRowType() {
		return rowType;
	}

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	public boolean shouldOutput(final MaxwellOutputConfig outputConfig) {
		return !suppressed;
	}

	public LinkedHashMap<String, Object> getData() {
		return data;
	}

	public LinkedHashMap<String, Object> getExtraAttributes() {
		return extraAttributes;
	}

	public LinkedHashMap<String, Object> getOldData() {
		return oldData;
	}

	public void suppress() {
		suppressed = true;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(final String topic) {
		kafkaTopic = topic;
	}

	public enum KeyFormat {HASH, ARRAY}
}
