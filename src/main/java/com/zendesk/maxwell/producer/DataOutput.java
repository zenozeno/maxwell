package com.zendesk.maxwell.producer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Enum to indicate if the data parameter should be the unique parameter in the output
 * <p>
 * example:
 * <p>
 * data output = false
 * <p>
 * {
 * "database": "test",
 * "table": "maxwell",
 * "type": "insert",
 * "ts": 1449786310,
 * "xid": 940752,
 * "commit": true,
 * "data": { "id":1, "daemon": "Stanislaw Lem" }
 * }
 * <p>
 * data output = true
 * <p>
 * {
 * "data": { "id":1, "daemon": "Stanislaw Lem" }
 * }
 * <p>
 * data output = content
 * <p>
 * {
 * "id":1,
 * "daemon": "Stanislaw Lem"
 * }
 */
public enum DataOutput {
	TRUE,
	FALSE,
	CONTENT;

	static Map<String, DataOutput> VALUES = Collections.unmodifiableMap(new HashMap<String, DataOutput>() {{
		put(TRUE.toString(), TRUE);
		put(FALSE.toString(), FALSE);
		put(CONTENT.toString(), CONTENT);
	}});

	public static DataOutput forValue(final String value) {
		final String val = Objects.nonNull(value) ? value.toUpperCase() : value;
		if (!VALUES.containsKey(val)) {
			throw new IllegalArgumentException("No matching constant for [" + value + "]");
		}
		return VALUES.get(val);
	}

}
