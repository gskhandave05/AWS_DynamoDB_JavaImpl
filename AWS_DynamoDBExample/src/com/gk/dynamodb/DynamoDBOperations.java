/**
 * 
 */
package com.gk.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;

/**
 * @author gauravkhandave
 *
 */
public class DynamoDBOperations {

	private static AmazonDynamoDBClient dynamoDbClient;

	/**
	 * Establishes connection between application and dynamoDB with user credentials. 
	 */
	private static void init() {

		AWSCredentials credentials;
		try {
			credentials = new ProfileCredentialsProvider("GauravKhandave").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/gauravkhandave/.aws/credentials), and is in valid format.", e);
		}
		dynamoDbClient = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDbClient.setRegion(usWest2);

	}

	/**
	 * Creates a new item for inserting in the table
	 * @param semester
	 * @param gpa
	 * @param subjects
	 * @return map object with string as a key and AttributeValue as a value
	 */
	private static Map<String, AttributeValue> newItem(String semester, Double gpa, String... subjects) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("semester", new AttributeValue(semester));
		item.put("gpa", new AttributeValue().withN(Double.toString(gpa)));
		item.put("fans", new AttributeValue().withSS(subjects));

		return item;
	}

	/**
	 * Describes the table
	 * @param tableName
	 */
	private static void describeTable(String tableName) {
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDbClient.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);
	}

	/**
	 * Adds items to the table
	 * @param tableName
	 * @param items
	 */
	private static void addItems(String tableName, List<Map<String, AttributeValue>> items) {
		for (Map<String, AttributeValue> item : items) {
			PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
			PutItemResult putItemResult = dynamoDbClient.putItem(putItemRequest);
			System.out.println("Result: " + putItemResult);
		}
	}

	/**
	 * Scans items from table by gpa
	 * @param tableName
	 * @param gpa
	 */
	private static void scanItemsByGpa(String tableName, Double gpa) {

		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
		Condition condition = new Condition().withComparisonOperator(ComparisonOperator.GT.toString())
				.withAttributeValueList(new AttributeValue().withN(Double.toString(gpa)));
		scanFilter.put("gpa", condition);
		ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
		ScanResult scanResult = dynamoDbClient.scan(scanRequest);
		System.out.println("Result: " + scanResult);

	}

	public static void main(String[] args) throws TableNeverTransitionedToStateException, InterruptedException {

		init();

		String tableName = "my-courses";

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName("semester").withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName("semester")
						.withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

		// Create table if it does not exist yet
		TableUtils.createTableIfNotExists(dynamoDbClient, createTableRequest);
		// wait for the table to move into ACTIVE state
		TableUtils.waitUntilActive(dynamoDbClient, tableName);

		// Describe Table
		describeTable(tableName);

		// Add items
		List<Map<String, AttributeValue>> items = new ArrayList<>();
		items.add(newItem("spring2016", 3.0, "Data Mining", "AI", "Machine Learning"));
		items.add(newItem("fall2016", 4.0, "Data Science", "Web Services", "Big Data"));
		items.add(newItem("spring2017", 3.9, "NoSQL", "Web Design", "Cloud Computing"));
		addItems(tableName, items);

		// Scan items for gpa greater than 3.5
		scanItemsByGpa(tableName, new Double(3.5));

	}

}
