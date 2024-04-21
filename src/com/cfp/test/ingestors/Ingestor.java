package com.cfp.test.ingestors;

import com.azure.cosmos.CosmosAsyncContainer;

public interface Ingestor {

    void ingestDocuments(CosmosAsyncContainer cosmosAsyncContainer, int docCount);

}
