package com.cfp.runners;

import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.beust.jcommander.Parameter;

public class Configuration {

    private static final int DEFAULT_FEED_CONTAINER_INITIAL_THROUGHPUT = 5000;
    private static final String DEFAULT_DATABASE_ID = "ffcf-abhm-cfp-db";
    private static final int DEFAULT_DOC_SIZE_IN_KB = 1;
    private static final boolean DEFAULT_SHOULD_FEED_CONTAINER_SPLIT = false;
    private static final int DEFAULT_FEED_CONTAINER_NEW_PROVISIONED_THROUGHPUT = 12_000;


    @Parameter(names = "-serviceEndpoint", description = "Service Endpoint")
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "Master Key")
    private String masterKey;

    @Parameter(names = "-feedContainerInitialThroughput", description = "Initial provisioned throughput for feed container")
    private int feedContainerInitialThroughput;

    @Parameter(names = "-databaseId", description = "Database id")
    private String databaseId;

    @Parameter(names = "-shouldFeedContainerSplit", description = "Flag indicating whether feed container should split", arity = 1)
    private boolean shouldFeedContainerSplit;

    @Parameter(names = "-shouldResetLeaseContainer", description = "Flag indicating whether lease container should be reset or not", arity = 1)
    private boolean shouldResetLeaseContainer;

    @Parameter(names = "-docCountToIngestBeforeSplit", description = "Count of documents to ingest before the split")
    private int docCountToIngestBeforeSplit;

    @Parameter(names = "-docCountToIngestAfterSplit", description = "Count of documents to ingest after the split")
    private int docCountToIngestAfterSplit;

    @Parameter(names = "-feedContainerNewProvisionedThroughput", description = "New provisioned throughput of feed container")
    private int feedContainerNewProvisionedThroughput;

    @Parameter(names = "-approximateDocSizeInBytes", description = "Approximate document size in bytes")
    private int approximateDocSizeInKB;

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

    public void populateWithDefaults() {
        this.serviceEndpoint = StringUtils.defaultString(this.serviceEndpoint, TestConfigurations.HOST);
        this.masterKey = StringUtils.defaultString(this.masterKey, TestConfigurations.MASTER_KEY);
        this.feedContainerInitialThroughput = DEFAULT_FEED_CONTAINER_INITIAL_THROUGHPUT;
        this.databaseId = StringUtils.defaultString(this.databaseId, DEFAULT_DATABASE_ID);
        this.shouldFeedContainerSplit = DEFAULT_SHOULD_FEED_CONTAINER_SPLIT;
        this.feedContainerNewProvisionedThroughput = DEFAULT_FEED_CONTAINER_NEW_PROVISIONED_THROUGHPUT;
        this.approximateDocSizeInKB = DEFAULT_DOC_SIZE_IN_KB;
    }
}
