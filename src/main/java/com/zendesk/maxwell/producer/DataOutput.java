package com.zendesk.maxwell.producer;

/**
 * Enum to indicate if the data parameter should be the unique parameter in the output
 *
 * example:
 *
 * data output = false
 *
 *   maxwell: {
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
 *   maxwell: {
 *	 	"data": { "id":1, "daemon": "Stanislaw Lem" }
 *	 }
 *
 * data output = content
 *
 *   maxwell: {
 *	 	"id":1,
 *	 	"daemon": "Stanislaw Lem"
 *	 }
 *
 */
public enum DataOutput {

	TRUE("true"),
	FALSE("false"),
	CONTENT("content");

	private final String value;

	DataOutput(String value) {
		this.value = value;
	}

	public static DataOutput forValue(final String value) {
		for (DataOutput dataOutput : values()) {
			if (dataOutput.value.equalsIgnoreCase(value)) {
				return dataOutput;
			}
		}
		throw new IllegalArgumentException("No matching constant for [" + value + "]");
	}

}
