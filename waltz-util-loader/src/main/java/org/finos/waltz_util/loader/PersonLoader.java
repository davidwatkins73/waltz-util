package org.finos.waltz_util.loader;

import org.finos.waltz_util.common.DIBaseConfiguration;
import org.finos.waltz_util.common.helper.DiffResult;
import org.finos.waltz_util.schema.tables.records.PersonRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.finos.waltz_util.common.helper.JacksonUtilities.getJsonMapper;
import static org.finos.waltz_util.schema.Tables.ORGANISATIONAL_UNIT;
import static org.finos.waltz_util.schema.Tables.PERSON;


public class PersonLoader {
    /**
     * Plan is:
     * 1. Load new people.
     * 2. Load DB data again, and do new comparison with email : ID comparisons
     * 3. Set Is_Removed for people who are not in the new data.
     *
     */

    private final static Long ORPHAN_ORG_UNIT_ID = 150L; // this is a constant, so should be declared as static and uppercased
    private final String resource;  // these are initialised at construction, and should not change - so mark as final
    private final DSLContext dsl;

    public PersonLoader(String resource){
        this.resource = resource;
        AnnotationConfigApplicationContext springContext = new AnnotationConfigApplicationContext(DIBaseConfiguration.class);
        dsl = springContext.getBean(DSLContext.class);
    }



    // clearer method name
    public void synch(){
        // updates and insertions should be under a single tx
        dsl.transaction(ctx -> {
            DSLContext tx = ctx.dsl();

            // pulled out the common code into this method
            Map<String, Long> orgIdByOrgExtId = loadOrgIdByExtIdMap(tx);

            Set<PersonOverview> existingPeople = tx
                    .select(PERSON.fields())
                    .select(ORGANISATIONAL_UNIT.EXTERNAL_ID, ORGANISATIONAL_UNIT.ID)
                    .from(PERSON)
                    .innerJoin(ORGANISATIONAL_UNIT)
                    .on(ORGANISATIONAL_UNIT.ID.eq(PERSON.ORGANISATIONAL_UNIT_ID))
                    .fetch()
                    .stream()
                    .map(r -> toDomain(r))
                    .collect(Collectors.toSet());

            Set<PersonOverview> desiredPeople = loadPeopleFromFile(orgIdByOrgExtId,
                                                                   mkEmailToEmployeeIdMap(existingPeople));

            // only need to do the diff once
            DiffResult<PersonOverview> diff = DiffResult.mkDiff(
                    existingPeople,
                    desiredPeople,
                    PersonOverview::employeeId,
                    Object::equals);


            // then use the bits of the diff for the appropriate 'handler' methods
            insertNew(tx, diff.otherOnly());  // java methods almost always start with a lowercase letter
            updateRelationships(tx, diff.differingIntersection());

            throw new IOException("test - kaboom!"); // rollback!

        });
    }


    private void insertNew(DSLContext tx, Collection<PersonOverview> toInsert) throws IOException {

        // minor point: /** is used for formal javadoc.  /* is used for multiline comments.
        // I prefer to just used // for multi-line (non javadoc)

        /*
         * 1. Get Current People from DB
         * 2. Get New people from JSON
         * 3. Compare emails (unique employee id if possible) todo: check for UUIDs
         * 4. insert new entries where no email match
         */

        List<PersonRecord> recordsToInsert = toInsert
                .stream()
                .map(p -> {
                    PersonRecord record = toJooqRecord(tx, p);
                    record.changed(PERSON.ID, false);
                    return record;})
                .collect(Collectors.toList());

        int recordsCreated = summarizeResults(tx
                .batchInsert(recordsToInsert)
                .execute());

        System.out.printf("Created: %d records\n", recordsCreated);
    }


    private void updateRelationships(DSLContext tx,
                                     Collection<PersonOverview> toUpdate) {
        /**
         * for all entries, compare with JSON
         * todo finish this bit
         */

        /**
         * 1. get email -> manager ID relationships from DB
         * 2. load new people from DB to create Existing PersonOverviews
         * 3. load new people from JSON to create Desired PersonOverviews
         * 4. compare the two sets of PersonOverviews
         * 5. update IntersectingDifferent entries
         * 6. set is_removed for rest
         */

        List<PersonRecord> recordsToUpdate = toUpdate
                .stream()
                .map(p -> {
                    PersonRecord record = toJooqRecord(tx, p);
                    record.changed(PERSON.ID, false);
                    return record;
                })
               .collect(Collectors.toList());

        int recordsUpdated = summarizeResults(tx   // no point splitting decl and assignment
                .batchUpdate(recordsToUpdate)
                .execute());

        System.out.printf("Updated: %d records\n", recordsUpdated);

    }


    private Set<PersonOverview> loadPeopleFromFile(Map<String, Long> orgIdByOrgExtId,
                                                   Map<String, String> emailToEmployeeID) throws IOException {
        InputStream resourceAsStream = PersonLoader.class.getClassLoader().getResourceAsStream(resource);
        PersonOverview[] rawOverviews = getJsonMapper().readValue(resourceAsStream, PersonOverview[].class);
        return Stream
                .of(rawOverviews)
                .map(d -> ImmutablePersonOverview
                        .copyOf(d)
                        .withOrganisationalUnitId(orgIdByOrgExtId.getOrDefault(d.organisationalUnitExternalId().toString(), ORPHAN_ORG_UNIT_ID))
                        // this bit is better written the other way around as d.managerEmail may be undefined:
                        // withManagerEmployeeId(emailToEmployeeID.getOrDefault(d.managerEmail().get(), "0")))
                        .withManagerEmployeeId(d.managerEmail()
                                .map(emailToEmployeeID::get)))
                .collect(Collectors.toSet());
    }


    private ImmutablePersonOverview toDomain(Record r){
        PersonRecord personRecord = r.into(PERSON);
        return ImmutablePersonOverview
                .builder()
                .employeeId(personRecord.getEmployeeId())
                .email(personRecord.getEmail())
                .displayName(personRecord.getDisplayName())
                .kind(personRecord.getKind())
                .managerEmployeeId(personRecord.getManagerEmployeeId())
                .title(personRecord.getTitle())
                .departmentName(Optional.ofNullable(personRecord.getDepartmentName()))
                .mobilePhone(Optional.ofNullable(personRecord.getMobilePhone()))
                .officePhone(Optional.ofNullable(personRecord.getOfficePhone()))
                .organisationalUnitId(personRecord.getOrganisationalUnitId())
                .organisationalUnitExternalId(r.get(ORGANISATIONAL_UNIT.EXTERNAL_ID))
                .build();
    }


    private PersonRecord toJooqRecord(DSLContext dsl, PersonOverview domain){
        PersonRecord record = dsl.newRecord(PERSON);
        record.setEmployeeId(domain.employeeId());
        record.setDisplayName(domain.displayName());
        record.setEmail(domain.email());
        record.setDepartmentName(domain.departmentName().orElse(null));
        record.setKind(domain.kind());
        // sets unmanaged people to 0 on every insert statement where its not specified, as it will be updated on second pass.
        record.setManagerEmployeeId(domain.managerEmployeeId().orElse("0"));
        record.setTitle(domain.title().orElse(null));
        record.setMobilePhone(domain.mobilePhone().orElse(null));
        record.setOfficePhone(domain.officePhone().orElse(null));
        record.setOrganisationalUnitId(domain.organisationalUnitId().orElse(ORPHAN_ORG_UNIT_ID)); // perhaps use the orphan group here?
        return record;

    }

    private static int summarizeResults(int[] rcs) {
        return IntStream.of(rcs).sum();
    }


    private Map<String, String> mkEmailToEmployeeIdMap(Set<PersonOverview> existingPeople) {
        return existingPeople
                .stream()
                .collect(Collectors.toMap(
                        PersonOverview::email,
                        PersonOverview::employeeId));
    }


    private static Map<String, Long> loadOrgIdByExtIdMap(DSLContext tx) {
        Map<String, Long> orgIdByOrgExtId = tx
                .select(ORGANISATIONAL_UNIT.EXTERNAL_ID, ORGANISATIONAL_UNIT.ID)
                .from(ORGANISATIONAL_UNIT)
                .fetchMap(ORGANISATIONAL_UNIT.EXTERNAL_ID, ORGANISATIONAL_UNIT.ID);
        return orgIdByOrgExtId;
    }


    public static void main(String[] args) {
        new PersonLoader("person.json").synch();
    }

}
