package debezium.payload.header.transformer;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Optional;

public class EnrichHeaderConfig extends AbstractConfig {

    public static final String PAYLOAD_FIELD_NAMES = "payload.fields";
    private static final String FIELD_NAME_DOC =
            "The name of the field which should be taken from payload and added to header. "
                    + "If null or empty won't be set";

    EnrichHeaderConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        PAYLOAD_FIELD_NAMES,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        FIELD_NAME_DOC);
    }

    Optional<String[]> payloadFields() {
        final String fieldsFromPayloadStr = getString(PAYLOAD_FIELD_NAMES);
        if (null == fieldsFromPayloadStr || "".equals(fieldsFromPayloadStr)) {
            return Optional.empty();
        }
        String[] fieldsFromPayload = fieldsFromPayloadStr.split(",");

        return Optional.of(fieldsFromPayload);
    }


}
