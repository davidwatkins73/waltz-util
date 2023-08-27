package org.finos.waltz_util.loader;

import org.finos.waltz_util.common.helper.ApplicationHelper;
import org.finos.waltz_util.common.DIBaseConfiguration;
import org.jooq.DSLContext;
import org.jooq.Record5;
import org.jooq.Result;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;

import static org.finos.waltz_util.schema.Tables.APPLICATION;
import static org.finos.waltz_util.schema.Tables.ORGANISATIONAL_UNIT;

public class Hello {

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(DIBaseConfiguration.class);

        DSLContext dsl = ctx.getBean(DSLContext.class);

        Result<Record5<String, Long, String, String, String>> result = dsl
                .select(APPLICATION.NAME,
                        APPLICATION.ID,
                        APPLICATION.ASSET_CODE,
                        ORGANISATIONAL_UNIT.NAME,
                        ORGANISATIONAL_UNIT.EXTERNAL_ID)
                .from(APPLICATION)
                .innerJoin(ORGANISATIONAL_UNIT)
                    .on(ORGANISATIONAL_UNIT.ID.eq(APPLICATION.ORGANISATIONAL_UNIT_ID))
                .fetch();

        Map<String, Long> appExtIdToIdMap = ApplicationHelper.loadExternalIdToIdMap(dsl);


        System.out.println(result);
    }

}
