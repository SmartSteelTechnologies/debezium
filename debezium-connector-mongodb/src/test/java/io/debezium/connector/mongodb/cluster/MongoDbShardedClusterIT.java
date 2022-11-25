/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.cluster;

import static io.debezium.connector.mongodb.cluster.MongoDbShardedCluster.shardedCluster;

import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;

/**
 * @see <a href="https://issues.redhat.com/browse/DBZ-5857">DBZ-5857</a>
 */
public class MongoDbShardedClusterIT {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testCluster() {
        try (var cluster = shardedCluster().shardCount(1).replicaCount(1).routerCount(1).build()) {
            logger.info("Starting {}...", cluster);
            cluster.start();

            String readPreference = "primary";
            var connectionString = new ConnectionString(cluster.getConnectionString() + "/?readPreference=" + readPreference);
            logger.info("Connecting to cluster: {}", connectionString);
            try (var client = MongoClients.create(connectionString)) {
                logger.info("Connected to cluster: {}", client.getClusterDescription());

                var databaseName = "test";
                cluster.enableSharding(databaseName); // Only needed in 5.0

                var collectionName = "docs";
                cluster.shardCollection(databaseName, collectionName, "name");

                var collection = client.getDatabase(databaseName).getCollection(collectionName);
                for (int i = 1; i <= 10; i++) {
                    collection.insertOne(Document.parse("{name:" + i + "}"));
                }

                for (var doc : collection.find()) {
                    logger.info("Doc: {}", doc);
                }

                logger.info("Connected to cluster: {}", client.getClusterDescription());
                cluster.addShard();
                logger.info("Connected to cluster: {}", client.getClusterDescription());
                cluster.removeShard();
            }
        }
    }

}
