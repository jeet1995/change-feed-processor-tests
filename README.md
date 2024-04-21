
1. Build `azure-cosmos`

- Clone the `jeet1995:azure-sdk-for-java` repository and switch to the `CfpTesting` branch.
- Build the `azure-cosmos` module with the following command if on Windows:

```
mvn --% -e -Ppackage-assembly -Dgpg.skip -DskipTests -Dmaven.javadoc.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drevapi.skip=true -pl com.azure:azure-cosmos clean package install
```
- Build the `azure-cosmos` module with the following command if on Linux:
```
mvn -e -Ppackage-assembly -Dgpg.skip -DskipTests -Dmaven.javadoc.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drevapi.skip=true -pl com.azure:azure-cosmos clean package install
```

2. Build `change-feed-processor-tests`

- Clone the `jeet1995:change-feed-processor-tests` repository and switch to the `master` branch.
- Build the `change-feed-processor-tests` module with the following command:

```
mvn -e -Ppackage-assembly clean package
```

3. Run the `jar` after building `change-feed-processor-tests`

3.1 Configurations possible

| Configuration                           | Configuration Description                                                                           | Possible values         |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------|-------------------------|
| `serviceEndpoint`                       | The Cosmos DB account URL.                                                                          | The relevant string     |
| `masterKey`                             | The primary key used to authenticate with the Cosmos DB account                                     | The relevant string     |
| `feedContainerInitialThroughput`        | Initial manual provisioned throughput of the feed container                                         | Any integer             |
| `databaseId`                            | The name based ID of the database                                                                   | Some string             |
| `feedContainerId`                       | The name based ID of the feed container.                                                            | Some string             |
| `leaseContainerId`                      | The name based ID of the lease container.                                                           | Some string             |
| `shouldFeedContainerSplit`              | Flag indicating whether feed container should split                                                 | `true` or `false`       |
| `shouldResetLeaseContainer`             | Flag indicating whether lease container should be reset or not                                      | `true` or `false`       |
| `docCountToIngestBeforeSplit`           | Count of documents to ingest before the split                                                       | Any integer             |
| `docCountToIngestAfterSplit`            | Count of documents to ingest after the split                                                        | Any integer             |
| `feedContainerNewProvisionedThroughput` | New manual provisioned throughput of the feed container to force splits of its physical partitions. | Any integer             |
| `bulkIngestionMicroBatchSize`           | Bulk ingestion micro-batch size                                                                     | Any integer             |
| `ingestionType`                         | Determines the way to ingest into the feed container - either through bulk of point creates.        | `Bulk` or `PointCreate` |

3.2 Running the `jar`

```
java -jar change-feed-processor-tests-1.0-SNAPSHOT-jar-with-dependencies.jar -serviceEndpoint "" -masterKey "" -feedContainerInitialThroughput 6000 -feedContainerId "" -leaseContainerId "" -shouldFeedContainerSplit "true" -shouldResetLeaseContainer "true" -docCountToIngestBeforeSplit 10000 -docCountToIngestAfterSplit 5000 -feedContainerNewProvisionedThroughput 11000 -bulkIngestionMicroBatchSize 50 -ingestionType "Bulk"
```