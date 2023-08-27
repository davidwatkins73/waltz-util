package org.finos.waltz_util.common.helper;

import org.jooq.DSLContext;

import java.util.Map;

import static org.finos.waltz_util.schema.Tables.APPLICATION;

public class ApplicationHelper {

    public static Map<String, Long> loadExternalIdToIdMap(DSLContext dsl) {
        return HelperUtils.loadExternalIdToIdMap(
                dsl,
                APPLICATION,
                APPLICATION.ID,
                APPLICATION.ASSET_CODE);
    }
}
