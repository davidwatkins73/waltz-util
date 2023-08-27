package org.finos.waltz_util.common.helper;

import org.jooq.DSLContext;

import java.util.Map;

import static org.finos.waltz_util.schema.Tables.ORGANISATIONAL_UNIT;

public class OrgUnitHelper {

    public static Map<String, Long> loadExternalIdToIdMap(DSLContext dsl) {
        return HelperUtils.loadExternalIdToIdMap(
                dsl,
                ORGANISATIONAL_UNIT,
                ORGANISATIONAL_UNIT.ID,
                ORGANISATIONAL_UNIT.EXTERNAL_ID);
    }
}
