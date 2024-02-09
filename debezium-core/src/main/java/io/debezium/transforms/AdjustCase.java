/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * This Transformation allows us to change a record's topic and field names to lower or upper case.
 * <p>
 * This makes it possible to e.g. make oracle table and column names postgres compatible.
 * <p>
 * The code is based on ByLogicalTableRouter
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 */
public class AdjustCase<R extends ConnectRecord<R>> implements Transformation<R> {

    /**
     * The set of options how the case of a string can be converted
     */
    public enum AdjustCaseTo implements EnumeratedValue {

        /**
         * Convert to lower case
         */
        LOWER("lower"),

        /**
         * Convert to upper case
         */
        UPPER("upper"),

        /**
         * No changes
         */
        KEEP("keep");

        private final String value;

        AdjustCaseTo(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static AdjustCaseTo parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            for (AdjustCaseTo option : AdjustCaseTo.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }
    }


    /**
     * The set of options how the case of a string can be converted
     */
    public enum MessageStyle implements EnumeratedValue {

        /**
         * Convert debezium style messages
         */
        DEBEZIUM("debezium"),

        /**
         * Convert kafka connect style messages
         */
        CONNECT("connect");


        private final String value;

        MessageStyle(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static MessageStyle parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            for (MessageStyle option : MessageStyle.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }
    }


    private static final Field TOPIC_ADJUST_CASE = Field.create("topic.name.case.to")
            .withDisplayName("Change case of the topic name")
            .withEnum(AdjustCaseTo.class, AdjustCaseTo.KEEP)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How the case of the topic name should be changed.");

    private static final Field FIELD_NAME_ADJUST_CASE = Field.create("field.name.case.to")
            .withDisplayName("Change case of the field names")
            .withEnum(AdjustCaseTo.class, AdjustCaseTo.KEEP)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How the case of the field names should be changed.");

    private static final Field MESSAGE_STYLE = Field.create("message.style")
            .withDisplayName("Whether the message is in \"debezium\" or in \"connect\" style")
            .withEnum(MessageStyle.class)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Both messages in \"debezium\" style with an envelope containing before and after fields and plain \"connect\" style messages that directly contain the new values as fields are supported.");

    private static final Logger LOGGER = LoggerFactory.getLogger(AdjustCase.class);

    private final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
    private AdjustCaseTo topicAdjustCaseTo;
    private AdjustCaseTo fieldNameAdjustCaseTo;
    private MessageStyle messageStyle;
    private final Cache<Schema, Schema> keySchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    private final Cache<Schema, Schema> envelopeSchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    private final Cache<String, String> topicReplaceCache = new SynchronizedCache<>(new LRUCache<String, String>(16));
    private final Cache<String, String> fieldNameReplaceCache = new SynchronizedCache<>(new LRUCache<String, String>(16));
    private SmtManager<R> smtManager;

    @Override
    public void configure(Map<String, ?> props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(TOPIC_ADJUST_CASE);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        topicAdjustCaseTo = AdjustCaseTo.parse(config.getString(TOPIC_ADJUST_CASE));
        fieldNameAdjustCaseTo = AdjustCaseTo.parse(config.getString(FIELD_NAME_ADJUST_CASE));
        messageStyle = MessageStyle.parse(config.getString(MESSAGE_STYLE));

        smtManager = new SmtManager<>(config);
    }

    @Override
    public R apply(R record) {
        final String oldTopic = record.topic();
        final String newTopic = determineNewTopic(oldTopic);

        LOGGER.debug("Applying topic name transformation from {} to {}", oldTopic, newTopic);

        Schema newKeySchema = null;
        Struct newKey = null;

        // Key could be null in the case of a table without a primary key
        if (record.key() != null) {
            final Struct oldKey = requireStruct(record.key(), "Updating schema");
            LOGGER.trace("Old key schema: {}", oldKey.schema());
            LOGGER.trace("Old key: {}", oldKey);
            newKeySchema = updatePlainMessageSchema(oldKey.schema(), newTopic);
            LOGGER.trace("New key schema: {}", newKeySchema);
            newKey = updatePlainMessage(newKeySchema, oldKey);
            LOGGER.trace("New key: {}", newKey);
        }

        // In case of tombstones or non-CDC events (heartbeats, schema change events),
        // leave the value as-is
        if (record.value() == null || (messageStyle == MessageStyle.DEBEZIUM && !smtManager.isValidEnvelope(record))) {
            // Value will be null in the case of a delete event tombstone
            return record.newRecord(
                    newTopic,
                    record.kafkaPartition(),
                    newKeySchema,
                    newKey,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }

        final Struct oldMessage = requireStruct(record.value(), "Updating schema");
        LOGGER.trace("Old message schema: {}", oldMessage.schema());
        LOGGER.trace("Old message: {}", oldMessage);
        Schema newMessageSchema;
        Struct newMessage;
        if (messageStyle == MessageStyle.DEBEZIUM) {
            newMessageSchema = updateEnvelopeSchema(oldMessage.schema(), newTopic);
            newMessage = updateEnvelope(newMessageSchema, oldMessage);
        } else if (messageStyle == MessageStyle.CONNECT) {
            newMessageSchema = updatePlainMessageSchema(oldMessage.schema(), newTopic);
            newMessage = updatePlainMessage(newMessageSchema, oldMessage);
        }
        else {
            throw new UnsupportedOperationException(
                    "Message style: " + messageStyle.getValue() + " is not yet implemented!");
        }
        LOGGER.trace("New message schema: {}", newMessageSchema);
        LOGGER.trace("New message: {}", newMessage);

        return record.newRecord(
                newTopic,
                record.kafkaPartition(),
                newKeySchema,
                newKey,
                newMessageSchema,
                newMessage,
                record.timestamp());
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        Field.group(
                config,
                null,
                TOPIC_ADJUST_CASE);
        return config;
    }

    /**
     * Determine the new topic name.
     *
     * @param oldTopic the name of the old topic
     * @return return the new topic name
     */
    private String determineNewTopic(String oldTopic) {
        // Not sure if caching actually improves performance here
        String newTopic = topicReplaceCache.get(oldTopic);
        if (newTopic != null) {
            return newTopic;
        }
        else {
            NameProcessor processor = getNameProcessor(topicAdjustCaseTo);
            newTopic = processor.process(oldTopic);
            topicReplaceCache.put(oldTopic, newTopic);
            return newTopic;
        }
    }

    /**
    * Determine the new field name.
    *
    * @param oldFieldName the name of the old field
    * @return return the new field name
    */
    private String determineNewFieldName(String oldFieldName) {
        // Not sure if caching actually improves performance here
        String newFieldName = fieldNameReplaceCache.get(oldFieldName);
        if (newFieldName != null) {
            LOGGER.debug("Applying field name transformation from {} to {}", oldFieldName, newFieldName);
            return newFieldName;
        }
        else {
            NameProcessor processor = getNameProcessor(fieldNameAdjustCaseTo);
            newFieldName = processor.process(oldFieldName);
            fieldNameReplaceCache.put(oldFieldName, newFieldName);
            LOGGER.debug("Applying field name transformation from {} to {}", oldFieldName, newFieldName);
            return newFieldName;
        }
    }

    private interface NameProcessor {
        String process(String oldTopic);
    }

    private class UpperCaseProcessor implements NameProcessor {
        public String process(String oldTopic) {
            return oldTopic.toUpperCase();
        }
    }

    private class LowerCaseProcessor implements NameProcessor {
        public String process(String oldTopic) {
            return oldTopic.toLowerCase();
        }
    }

    private class KeepCaseProcessor implements NameProcessor {
        public String process(String oldTopic) {
            return oldTopic.toLowerCase();
        }
    }

    private NameProcessor getNameProcessor(AdjustCaseTo caseChange) {
        if (topicAdjustCaseTo == AdjustCaseTo.LOWER) {
            return new LowerCaseProcessor();
        }
        else if (topicAdjustCaseTo == AdjustCaseTo.UPPER) {
            return new UpperCaseProcessor();
        }
        else if (topicAdjustCaseTo == AdjustCaseTo.KEEP) {
            return new KeepCaseProcessor();
        }
        else {
            throw new UnsupportedOperationException(
                    "Name processor for caseChange: " + caseChange.getValue() + " is not yet implemented!");
        }
    }

    private Schema updatePlainMessageSchema(Schema oldKeySchema, String newTopicName) {
        Schema newKeySchema = keySchemaUpdateCache.get(oldKeySchema);
        if (newKeySchema != null) {
            return newKeySchema;
        }

        final SchemaBuilder builder = copySchemaExcludingName(oldKeySchema, SchemaBuilder.struct(), false);
        builder.name(schemaNameAdjuster.adjust(newTopicName + ".Key"));

        for (org.apache.kafka.connect.data.Field field : oldKeySchema.fields()) {
            String newName = determineNewFieldName(field.name());
            builder.field(newName, field.schema());
        }

        newKeySchema = builder.build();
        keySchemaUpdateCache.put(oldKeySchema, newKeySchema);
        return newKeySchema;
    }

    private Struct updatePlainMessage(Schema newKeySchema, Struct oldKey) {
        final Struct newKey = new Struct(newKeySchema);
        for (org.apache.kafka.connect.data.Field field : oldKey.schema().fields()) {
            String newName = determineNewFieldName(field.name());
            newKey.put(newName, oldKey.get(field));
        }
        return newKey;
    }

    private Schema updateEnvelopeSchema(Schema oldEnvelopeSchema, String newTopicName) {
        Schema newEnvelopeSchema = envelopeSchemaUpdateCache.get(oldEnvelopeSchema);
        if (newEnvelopeSchema != null) {
            return newEnvelopeSchema;
        }

        // Build new schema based on the BEFORE field. It will be used for the after field as well.
        final Schema oldValueSchema = oldEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
        final SchemaBuilder ValueBuilder = copySchemaExcludingName(oldValueSchema, SchemaBuilder.struct(), false);
        ValueBuilder.name(schemaNameAdjuster.adjust(newTopicName + ".Value"));
        for (org.apache.kafka.connect.data.Field field : oldValueSchema.fields()) {
            String newName = determineNewFieldName(field.name());
            ValueBuilder.field(newName, field.schema());
        }
        final Schema newValueSchema = ValueBuilder.build();

        // Apply the new schema to both the before and after field
        final SchemaBuilder envelopeBuilder = copySchemaExcludingName(oldEnvelopeSchema, SchemaBuilder.struct(), false);
        for (org.apache.kafka.connect.data.Field field : oldEnvelopeSchema.fields()) {
            final String fieldName = field.name();
            Schema fieldSchema = field.schema();
            if (Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(fieldName, Envelope.FieldName.AFTER)) {
                fieldSchema = newValueSchema;
            }
            envelopeBuilder.field(fieldName, fieldSchema);
        }
        envelopeBuilder.name(schemaNameAdjuster.adjust(Envelope.schemaName(newTopicName)));

        newEnvelopeSchema = envelopeBuilder.build();
        envelopeSchemaUpdateCache.put(oldEnvelopeSchema, newEnvelopeSchema);
        return newEnvelopeSchema;
    }

    private Struct updateEnvelope(Schema newEnvelopeSchema, Struct oldEnvelope) {
        final Struct newEnvelope = new Struct(newEnvelopeSchema);
        final Schema newValueSchema = newEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
        for (org.apache.kafka.connect.data.Field field : oldEnvelope.schema().fields()) {
            final String fieldName = field.name();
            Object fieldValue = oldEnvelope.get(field);
            if ((Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(fieldName, Envelope.FieldName.AFTER))
                    && fieldValue != null) {
                fieldValue = updateValue(newValueSchema, requireStruct(fieldValue, "Updating schema"));
            }
            newEnvelope.put(fieldName, fieldValue);
        }

        return newEnvelope;
    }

    private Struct updateValue(Schema newValueSchema, Struct oldValue) {
        final Struct newValue = new Struct(newValueSchema);
        for (org.apache.kafka.connect.data.Field field : oldValue.schema().fields()) {
            String newName = determineNewFieldName(field.name());
            newValue.put(newName, oldValue.get(field));
        }
        return newValue;
    }

    private SchemaBuilder copySchemaExcludingName(Schema source, SchemaBuilder builder, boolean copyFields) {
        builder.version(source.version());
        builder.doc(source.doc());

        Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        if (source.isOptional()) {
            builder.optional();
        }
        else {
            builder.required();
        }

        if (copyFields) {
            for (org.apache.kafka.connect.data.Field field : source.fields()) {
                builder.field(field.name(), field.schema());
            }
        }

        return builder;
    }
}
