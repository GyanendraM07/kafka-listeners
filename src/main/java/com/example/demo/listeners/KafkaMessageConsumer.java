package com.example.demo.listeners;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaMessageConsumer {

	// This one is For Error
	@KafkaListener(topics = "TestTopic1", groupId = "my-consumer-group")
	public void listenToTestTopic1(ConsumerRecord<String, String> record) {
		String jsonMessage = record.value();
		System.out.println("Received message from TestTopic1 in Consumer1: " + jsonMessage);
		// Process the message here
	}

//	    @KafkaListener(topics = "TestTopic2",groupId = "my-consumer-group")
//	    public void listenToTestTopic2(String message) {
//	        System.out.println("Received message from TestTopic2: " + message);
//	        // Process the message here
//	    }
	private final ObjectMapper objectMapper;

	public KafkaMessageConsumer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	// This is FOr To store Data in Database
	@KafkaListener(topics = "TestTopic2", groupId = "my-consumer-group")
	public void listen(ConsumerRecord<String, String> record) {
		String jsonMessage = record.value();
		System.out.println(jsonMessage);
		try {
			// Deserialize the JSON data to a list of maps
			List<Map<String, String>> keyValueList = objectMapper.readValue(jsonMessage, List.class);
			String idColName = "assemb";
			String tableName = "bill_of_materials";
			// Print the deserialized data
			for (Map<String, String> keyValueMap : keyValueList) {
				System.out.println("Deserialized message:");
				StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + tableName + "(");
				StringBuilder valuesBuilder = new StringBuilder("VALUES (");
				StringBuilder conflictBuilder = new StringBuilder("ON CONFLICT (" + idColName + ") DO UPDATE SET ");
				StringBuilder whereBuilder = new StringBuilder("WHERE");
				boolean firstEntry = true;
				for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
					if (!firstEntry) {
						queryBuilder.append(", ");
						valuesBuilder.append(", ");
						conflictBuilder.append(", ");
						whereBuilder.append("OR");
					} else {
						firstEntry = false;
					}
					queryBuilder.append(entry.getKey()); // Append column name
					valuesBuilder.append("'").append(entry.getValue()).append("'");
					conflictBuilder.append(
							entry.getKey() + "=CASE WHEN" + tableName + "." + entry.getKey() + "<>" + entry.getValue()
									+ "THEN" + entry.getValue() + "ELSE " + tableName + "." + entry.getKey() + "END,");
					whereBuilder.append(tableName + "." + entry.getKey() + "<>" + entry.getValue());
				}
				queryBuilder.append("created_date,created_by )");
				valuesBuilder.append("CURRENT_TIMESTAMP,@System)"); // Close the VALUES part
				queryBuilder.append(valuesBuilder.toString());
				conflictBuilder.append("updated_date = CURRENT_TIMESTAMP,updated_by=@System");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


//private String buildUpsertQuery() {
//    return "INSERT INTO your_table (id, column1, column2, column3, column4, column5, created_date, created_by) " +
//           "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, @System) " +
//           "ON CONFLICT (id) DO UPDATE SET " +
//           "column1 = CASE WHEN your_table.column1 <> EXCLUDED.column1 THEN EXCLUDED.column1 ELSE your_table.column1 END, " +
//           "column2 = CASE WHEN your_table.column2 <> EXCLUDED.column2 THEN EXCLUDED.column2 ELSE your_table.column2 END, " +
//           "column3 = CASE WHEN your_table.column3 <> EXCLUDED.column3 THEN EXCLUDED.column3 ELSE your_table.column3 END, " +
//           "column4 = CASE WHEN your_table.column4 <> EXCLUDED.column4 THEN EXCLUDED.column4 ELSE your_table.column4 END, " +
//           "column5 = CASE WHEN your_table.column5 <> EXCLUDED.column5 THEN EXCLUDED.column5 ELSE your_table.column5 END, " +
//           "updated_date = CASE WHEN your_table.column1 <> EXCLUDED.column1 OR your_table.column2 <> EXCLUDED.column2 OR your_table.column3 <> EXCLUDED.column3 OR your_table.column4 <> EXCLUDED.column4 OR your_table.column5 <> EXCLUDED.column5 THEN CURRENT_TIMESTAMP ELSE your_table.updated_date END, " +
//           "updated_by = CASE WHEN your_table.column1 <> EXCLUDED.column1 OR your_table.column2 <> EXCLUDED.column2 OR your_table.column3 <> EXCLUDED.column3 OR your_table.column4 <> EXCLUDED.column4 OR your_table.column5 <> EXCLUDED.column5 THEN ? ELSE your_table.updated_by END " +
//           "WHERE your_table.column1 <> EXCLUDED.column1 OR your_table.column2 <> EXCLUDED.column2 OR your_table.column3 <> EXCLUDED.column3 OR your_table.column4 <> EXCLUDED.column4 OR your_table.column5 <> EXCLUDED.column5";
//}
