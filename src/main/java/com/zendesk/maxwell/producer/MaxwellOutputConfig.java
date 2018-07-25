package com.zendesk.maxwell.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MaxwellOutputConfig {
	public DataOutput outputDataOnly;
	public boolean includesBinlogPosition;
	public boolean includesGtidPosition;
	public boolean includesCommitInfo;
	public boolean includesXOffset;
	public boolean includesNulls;
	public boolean includesServerId;
	public boolean includesThreadId;
	public boolean includesRowQuery;
	public boolean outputDDL;
	public List<Pattern> excludeColumns;
	public EncryptionMode encryptionMode;
	public String secretKey;

	public MaxwellOutputConfig() {
		outputDataOnly = DataOutput.FALSE;
		includesBinlogPosition = false;
		includesGtidPosition = false;
		includesCommitInfo = true;
		includesNulls = true;
		includesServerId = false;
		includesThreadId = false;
		includesRowQuery = false;
		outputDDL = false;
		excludeColumns = new ArrayList<>();
		encryptionMode = EncryptionMode.ENCRYPT_NONE;
		secretKey = null;
	}

	public boolean encryptionEnabled() {
		return encryptionMode != EncryptionMode.ENCRYPT_NONE;
	}
}
