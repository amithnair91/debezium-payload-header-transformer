package debezium.payload.header.transformer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class EnrichHeaderWithEventType<R extends ConnectRecord<R>> implements Transformation<R> {

    private EnrichHeaderConfig config;

    /**
     * This method is invoked when a change is made on the outbox schema.
     *
     * @param sourceRecord
     * @return
     */
    public R apply(R sourceRecord) {
        Struct kStruct = (Struct) sourceRecord.value();
        String databaseOperation = kStruct.getString("op");
        //Publish events only during the Create operation
        if ("c".equalsIgnoreCase(databaseOperation) && config.payloadFields().isPresent()) {
            JsonObject payload = payload(sourceRecord);

            enrichRecordWithHeaderFromPayload(sourceRecord, payload, config.payloadFields().get());
            return sourceRecord;
        } else {
            return null;
        }
    }

    private void enrichRecordWithHeaderFromPayload(R sourceRecord, JsonObject payload, String[] fieldsInPayload) {
        Headers eventHeaders = sourceRecord.headers();

        for (String fieldName : fieldsInPayload) {
            JsonElement item = payload.get(fieldName);
            if (item == null || item.getAsString().length() < 1)
                throw new RuntimeException("Field " + fieldName + " must exist in the Event and must not be empty");
            eventHeaders.addString(fieldName, item.getAsString());
        }
    }

    private JsonObject payload(R sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        return new Gson().fromJson(value.getString("after"), JsonObject.class);
    }

    @Override
    public ConfigDef config() {
        return EnrichHeaderConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new EnrichHeaderConfig(configs);
    }

}