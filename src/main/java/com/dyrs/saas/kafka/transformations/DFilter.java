package com.dyrs.saas.kafka.transformations;

import com.jayway.jsonpath.*;
import io.confluent.connect.transforms.ExtractTopic;
import io.confluent.connect.transforms.Filter;
import io.confluent.connect.transforms.util.Requirements;
import io.confluent.connect.transforms.util.SimpleConfig;
import io.confluent.connect.transforms.util.TypeConverter;
import io.confluent.connect.utils.recommenders.Recommenders;
import io.confluent.connect.utils.validators.Validators;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class DFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOG = LoggerFactory.getLogger(DFilter.class);
    public static final String FILTER_CONDITION_CONFIG = "filter.condition";
    public static final String FILTER_CONDITION_DISPLAY = "Filter Condition";
    public static final String FILTER_TYPE_CONFIG = "filter.type";
    public static final String FILTER_TYPE_DISPLAY = "Filter Type";
    public static final String MISSING_OR_NULL_BEHAVIOR_CONFIG = "missing.or.null.behavior";
    public static final String MISSING_OR_NULL_BEHAVIOR_DISPLAY = "Handle Missing Or Null Fields";
    private static final String FILTER_USE_PURPOSE = "filtering record without schema";
    private static final String FILTER_USE_PURPOSE_WITH_SCHEMA = "filtering record with schema";
    private static final ConfigDef.Validator VALIDATOR_JSON_PATH = (name, value) -> {
        try {
            JsonPath.compile((String) value, new Predicate[0]);
        } catch (InvalidPathException var3) {
            throw new ConfigException(name, value, "Invalid json path defined. Please refer to https://github.com/json-path/JsonPath README for correct use of json path.");
        }
    };
    private SimpleConfig config;
    private JsonPath filterConditionPath;
    private DFilter.FilterType filterTypeEnum;
    private DFilter.MissingOrNullBehavior missingOrNullBehaviorEnum;
    public static final ConfigDef CONFIG_DEF;

    public DFilter() {
    }

    public void configure(Map<String, ?> props) {
        this.config = new SimpleConfig(CONFIG_DEF, props);
        this.filterConditionPath = JsonPath.compile(this.config.getString("filter.condition"), new Predicate[0]);
        this.filterTypeEnum = DFilter.FilterType.valueOf(this.config.getString("filter.type").toUpperCase());
        this.missingOrNullBehaviorEnum = DFilter.MissingOrNullBehavior.valueOf(this.config.getString("missing.or.null.behavior").toUpperCase());

        LOG.info("当前配置信息：", CONFIG_DEF.toHtml());
    }

    public R apply(R record) {
        LOG.info("判断是否符合条件：", record.topic());
        return this.operatingValue(record) != null && this.shouldDrop(record) ? null : record;
    }

    public void close() {
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R var1);

    protected abstract Object operatingValue(R var1);

    private boolean shouldDrop(R record) {
        Schema schema = this.operatingSchema(record);
        Object data = schema == null ? Requirements.requireMap(this.operatingValue(record), "filtering record without schema") : TypeConverter.convertObject(Requirements.requireStruct(this.operatingValue(record), "filtering record with schema"), ((Struct) this.operatingValue(record)).schema());

        try {
            List<?> filtered = (List) this.filterConditionPath.read(data, Configuration.defaultConfiguration().addOptions(new Option[]{Option.ALWAYS_RETURN_LIST}));
            switch (this.filterTypeEnum) {
                case EXCLUDE:
                    return filtered.size() != 0;
                case INCLUDE:
                default:
                    return filtered.size() == 0;
            }
        } catch (PathNotFoundException var5) {
            if (this.missingOrNullBehaviorEnum == DFilter.MissingOrNullBehavior.FAIL) {
                throw new PathNotFoundException("Unable to apply the JSON Path filter condition `" + this.config.getString("filter.condition") + "` because the path could not be found in the record. Set ``" + "missing.or.null.behavior" + "`` to ``" + DFilter.MissingOrNullBehavior.INCLUDE.name().toLowerCase() + "`` or ``" + DFilter.MissingOrNullBehavior.EXCLUDE.name().toLowerCase() + "`` to change how the transform handles this condition.");
            } else {
                return this.missingOrNullBehaviorEnum == DFilter.MissingOrNullBehavior.EXCLUDE;
            }
        }
    }

    static {
        CONFIG_DEF = (new ConfigDef()).define("filter.condition", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, VALIDATOR_JSON_PATH, ConfigDef.Importance.HIGH, "The criteria used to match records to be included or excluded by this transformation. Use JSON Path predicate notation defined in: https://github.com/json-path/JsonPath ", (String) null, -1, ConfigDef.Width.LONG, "Filter Condition").define("filter.type", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Validators.oneOf(DFilter.FilterType.class), ConfigDef.Importance.HIGH, "The action to perform with records that match the ``filter.condition`` predicate. Use ``" + DFilter.FilterType.INCLUDE.name().toLowerCase() + "`` to pass through all records that match the predicate and drop all records that do not satisfy the predicate, or use ``" + DFilter.FilterType.EXCLUDE.name().toLowerCase() + "`` to drop all records that match the predicate.", (String) null, -1, ConfigDef.Width.SHORT, "Filter Type", Recommenders.enumValues(DFilter.FilterType.class, new DFilter.FilterType[0])).define("missing.or.null.behavior", ConfigDef.Type.STRING, DFilter.MissingOrNullBehavior.FAIL.name().toLowerCase(), Validators.oneOf(DFilter.MissingOrNullBehavior.class), ConfigDef.Importance.MEDIUM, "The behavior when the record does not have the field(s) used in the ``filter.condition``. Use ``" + DFilter.MissingOrNullBehavior.FAIL.name().toLowerCase() + "`` to throw an exception and fail the connector task, ``" + DFilter.MissingOrNullBehavior.INCLUDE.name().toLowerCase() + "`` to pass the recordthrough, or ``" + DFilter.MissingOrNullBehavior.EXCLUDE.name().toLowerCase() + "``to drop the record.", (String) null, -1, ConfigDef.Width.SHORT, "Handle Missing Or Null Fields", Recommenders.enumValues(DFilter.MissingOrNullBehavior.class, new DFilter.MissingOrNullBehavior[0]));
    }

    public static class Value<R extends ConnectRecord<R>> extends Filter<R> {
        public Value() {
        }

        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        protected Object operatingValue(R record) {
            return record.value();
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends Filter<R> {
        public Key() {
        }

        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        protected Object operatingValue(R record) {
            return record.key();
        }
    }

    protected static enum MissingOrNullBehavior {
        FAIL,
        INCLUDE,
        EXCLUDE;

        private MissingOrNullBehavior() {
        }
    }

    protected static enum FilterType {
        INCLUDE,
        EXCLUDE;

        private FilterType() {
        }
    }
}

//    private static final Logger LOG = LoggerFactory.getLogger(DFilter.class);
//
//    @Override
//    protected Schema operatingSchema(R r) {
//        return null;
//    }
//
//    @Override
//    protected Object operatingValue(R r) {
//        return null;
//    }
//
//    @Override
//    public R apply(R record) {
//        LOG.info("判断是否符合条件：", record.topic());
//        return super.apply(record);
//    }
//
//    @Override
//    public void configure(Map<String, ?> props) {
//        super.configure(props);
//        LOG.info("当前配置信息：", CONFIG_DEF.toHtml());
//    }
//}
