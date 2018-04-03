package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;

import java.util.Objects;

public abstract class DatabaseChange extends SchemaChange{

	protected String database;

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return Objects.nonNull(filter) ? filter.isDatabaseBlacklisted(database): false;
	}

	@Override
	public boolean isWhitelisted(MaxwellFilter filter) {
		return Objects.nonNull(filter) ? filter.isDatabaseWhitelisted(database): true;
	}
}
