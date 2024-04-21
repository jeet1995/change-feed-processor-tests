package com.cfp.test;

import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.cfp.test.ingestors.IngestionType;

import java.util.Locale;

public class Configuration {

    @Parameter(names = "-serviceEndpoint", description = "Service Endpoint", required = true)
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "Master Key", required = true)
    private String masterKey;

    @Parameter(names = "-feedContainerInitialThroughput", description = "Initial provisioned throughput for feed container")
    private int feedContainerInitialThroughput = 6000;

    @Parameter(names = "-databaseId", description = "Database id")
    private String databaseId = "all-version-deletes-test-db";

    @Parameter(names = "-feedContainerId", description = "Feed container id")
    private String feedContainerId = "feed-container";

    @Parameter(names = "-leaseContainerId", description = "Lease container id")
    private String leaseContainerId = "lease-container";

    @Parameter(names = "-shouldFeedContainerSplit", description = "Flag indicating whether feed container should split", arity = 1)
    private boolean shouldFeedContainerSplit = false;

    @Parameter(names = "-shouldResetLeaseContainer", description = "Flag indicating whether lease container should be reset or not", arity = 1)
    private boolean shouldResetLeaseContainer = false;

    @Parameter(names = "-docCountToIngestBeforeSplit", description = "Count of documents to ingest before the split")
    private int docCountToIngestBeforeSplit = 6000;

    @Parameter(names = "-docCountToIngestAfterSplit", description = "Count of documents to ingest after the split")
    private int docCountToIngestAfterSplit = 6000;

    @Parameter(names = "-feedContainerNewProvisionedThroughput", description = "New provisioned throughput of feed container")
    private int feedContainerNewProvisionedThroughput = 11000;

    @Parameter(names = "-bulkIngestionMicroBatchSize", description = "Bulk ingestion micro-batch size")
    private int bulkIngestionMicroBatchSize = 50;

    @Parameter(names = "-changeFeedMaxItemCount", description = "Max item per change feed batch.")
    private int changeFeedMaxItemCount = 10;

    @Parameter(names = "-ingestionType", description = "Determines the way to ingest - either through bulk of point creates.", converter = IngestionTypeConverter.class)
    private IngestionType ingestionType = IngestionType.BULK;

    @Parameter(names = "-shouldCleanUpContainers", description = "Flag to indicate whether feed and lease containers should be deleted after a run.", arity = 1)
    private boolean shouldCleanUpContainers = false;

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public int getFeedContainerInitialThroughput() {
        return feedContainerInitialThroughput;
    }

    public String getDatabaseId() {
        return databaseId;
    }


    public String getFeedContainerId() {
        return feedContainerId;
    }

    public String getLeaseContainerId() {
        return leaseContainerId;
    }

    public boolean shouldFeedContainerSplit() {
        return shouldFeedContainerSplit;
    }

    public boolean shouldResetLeaseContainer() {
        return shouldResetLeaseContainer;
    }

    public int getDocCountToIngestBeforeSplit() {
        return docCountToIngestBeforeSplit;
    }

    public int getDocCountToIngestAfterSplit() {
        return docCountToIngestAfterSplit;
    }

    public int getFeedContainerNewProvisionedThroughput() {
        return feedContainerNewProvisionedThroughput;
    }

    public int getBulkIngestionMicroBatchSize() {
        return bulkIngestionMicroBatchSize;
    }

    public IngestionType getIngestionType() {
        return ingestionType;
    }

    public int getChangeFeedMaxItemCount() {
        return changeFeedMaxItemCount;
    }

    public boolean shouldCleanUpContainers() {
        return shouldCleanUpContainers;
    }

    static class IngestionTypeConverter implements IStringConverter<IngestionType> {

        @Override
        public IngestionType convert(String value) {

            if (value == null || value.isEmpty()) {
                return IngestionType.BULK;
            }

            String normalizedIngestionTypeConvertedAsString = value.toLowerCase(Locale.ROOT).trim();

            if (normalizedIngestionTypeConvertedAsString.equals("bulk")) {
                return IngestionType.BULK;
            } else if (normalizedIngestionTypeConvertedAsString.equals("pointcreate")) {
                return IngestionType.POINT_CREATE;
            }

            return IngestionType.BULK;
        }
    }

    public void populateWithDefaults() {
        this.serviceEndpoint = StringUtils.defaultString(this.serviceEndpoint, TestConfigurations.HOST);
        this.masterKey = StringUtils.defaultString(this.masterKey, TestConfigurations.MASTER_KEY);
    }
}
