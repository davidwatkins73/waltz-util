package org.finos.waltz_util.common.helper;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

import java.util.Map;

public class HelperUtils {

    public static Map<String, Long> loadExternalIdToIdMap(DSLContext dsl,
                                                          Table table,
                                                          Field<Long> idField,
                                                          Field<String> externalIdField) {

        return dsl.select(idField, externalIdField)
                  .from(table)
                  .fetchMap(externalIdField, idField);
    }
}
