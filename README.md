
1. Prerequisites

- Install JDK 17 and Maven.

2. Build `azure-cosmos`

- Clone the `jeet1995:azure-sdk-for-java` repository and switch to the `CfpTesting` branch.
- Navigate to `~<path-to-azure-sdk-for-java>/sdk/cosmos`
- Build the `azure-cosmos` module with the following command if on Windows:

```
mvn --% -e -Ppackage-assembly -Dgpg.skip -DskipTests -Dmaven.javadoc.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drevapi.skip=true -pl com.azure:azure-cosmos clean package install
```
- Build the `azure-cosmos` module with the following command if on Linux:
```
mvn -e -Ppackage-assembly -Dgpg.skip -DskipTests -Dmaven.javadoc.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drevapi.skip=true -pl com.azure:azure-cosmos clean package install
```

3. Build `change-feed-processor-tests`

- Clone the `jeet1995:change-feed-processor-tests` repository and switch to the `master` branch.
- Navigate to `~<path-to-change-feed-processor-tests>/sdk/cosmos`
- Build the `change-feed-processor-tests` module with the following command:

```
mvn -e -Ppackage-assembly clean package
```

4. Run the `jar` after building `change-feed-processor-tests`

-Possible Configurations

| Configuration                           | Configuration Description                                                                                                          | Possible values         | Defaults                                       |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-------------------------|------------------------------------------------|
| `serviceEndpoint`                       | The Cosmos DB account URL.                                                                                                         | The relevant string     | Setting this is compulsory.                    |
| `masterKey`                             | The primary key used to authenticate with the Cosmos DB account                                                                    | The relevant string     | Setting this is compulsory.                    |
| `feedContainerInitialThroughput`        | Initial manual provisioned throughput of the feed container                                                                        | Any integer             | 6000                                           |
| `databaseId`                            | The name based ID of the database                                                                                                  | Some string             | `all-version-deletes-test-db`                  |
| `feedContainerId`                       | The name based ID of the feed container.                                                                                           | Some string             | Defaults to a UUID prefix and `-feed` suffix.  |
| `leaseContainerId`                      | The name based ID of the lease container.                                                                                          | Some string             | Defaults to a UUID prefix and `-lease` suffix. |
| `shouldFeedContainerSplit`              | Flag indicating whether feed container should split                                                                                | `true` or `false`       | `false`                                        |
| `shouldResetLeaseContainer`             | Flag indicating whether lease container should be reset or not                                                                     | `true` or `false`       | `false`                                        |
| `docCountToIngestBeforeSplit`           | Count of documents to ingest before the split                                                                                      | Any integer             | 6000                                           |
| `docCountToIngestAfterSplit`            | Count of documents to ingest after the split                                                                                       | Any integer             | 6000                                           |
| `feedContainerNewProvisionedThroughput` | New manual provisioned throughput of the feed container to force splits of its physical partitions.                                | Any integer             | 11000                                          |
| `bulkIngestionMicroBatchSize`           | Bulk ingestion micro-batch size                                                                                                    | Any integer             | 50                                             |
| `ingestionType`                         | Determines the way to ingest into the feed container - either through bulk of point creates.                                       | `Bulk` or `PointCreate` | `Bulk`                                         |
| `changeFeedMaxItemCount`                | Determines the max item count per change feed enumeration - could go beyond the specified value depending on ingestion batch size. | Any integer.            | 10                                             |
| `shouldCleanUpContainers`               | Flag to indicate whether feed and lease containers should be deleted after the run.                                                | `true` or `false`       | `true`                                         |

3.2 Running the `jar`

- Navigate to the location `~<path-till-downloaded-clone>/change-feed-processor-tests/target`
```
java -jar change-feed-processor-tests-1.0-SNAPSHOT-jar-with-dependencies.jar -serviceEndpoint "" -masterKey "" -feedContainerInitialThroughput 6000 -feedContainerId "feed-container" -leaseContainerId "lease-container" -shouldFeedContainerSplit "true" -shouldResetLeaseContainer "true" -docCountToIngestBeforeSplit 10000 -docCountToIngestAfterSplit 5000 -feedContainerNewProvisionedThroughput 11000 -bulkIngestionMicroBatchSize 50 -ingestionType "Bulk" -changeFeedMaxItemCount 20 -shouldCleanUpContainers "false"
```