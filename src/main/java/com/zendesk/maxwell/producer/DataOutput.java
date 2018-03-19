package com.zendesk.maxwell.producer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum to indicate if the data parameter should be the unique parameter in the output
 *
 * example:
 *
 * data output = false
 *
 *   {
 *	 	"database": "test",
 *	 	"table": "maxwell",
 *	 	"type": "insert",
 *	 	"ts": 1449786310,
 *	 	"xid": 940752,
 *	 	"commit": true,
 *	 	"data": { "id":1, "daemon": "Stanislaw Lem" }
 *	 }
 *
 * data output = true
 *
 *   {
 *	 	"data": { "id":1, "daemon": "Stanislaw Lem" }
 *	 }
 *
 * data output = content
 *
 *   {
 *	 	"id":1,
 *	 	"daemon": "Stanislaw Lem"
 *	 }
 *
 */
public enum DataOutput {
	TRUE,
	FALSE,
	CONTENT;

	static Map<String, DataOutput> VALUES = Collections.unmodifiableMap(new HashMap<String , DataOutput>() {{
		put(TRUE.toString(), TRUE);
		put(FALSE.toString(), FALSE);
		put(CONTENT.toString(), CONTENT);
	}});

	public static DataOutput forValue(final String value) {
		if(!VALUES.containsKey(value)) {
			throw new IllegalArgumentException("No matching constant for [" + value + "]");
		}
		return VALUES.get(value);
	}

}
