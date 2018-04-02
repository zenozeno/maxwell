package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class DatabaseAlter extends DatabaseChange {
	public String charset;

	public DatabaseAlter(String database) {
		this.database = database;
	}

	@Override
	public ResolvedDatabaseAlter resolve(Schema s) throws InvalidSchemaError {
		return new ResolvedDatabaseAlter(this.database, this.charset);
	}

}
