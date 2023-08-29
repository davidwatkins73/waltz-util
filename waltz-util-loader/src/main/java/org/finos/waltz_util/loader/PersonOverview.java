package org.finos.waltz_util.loader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.Optional;


@Value.Immutable
@JsonSerialize(as = ImmutablePersonOverview.class)
@JsonDeserialize(as = ImmutablePersonOverview.class)
public abstract class PersonOverview {

    @JsonProperty("employee_id")
    public abstract String employeeId(); // fixed name, java tends to avoid underscores in class names and method names

    @JsonProperty("name")
    public abstract String displayName();

    public abstract String email();


    @JsonProperty("user_principal_name")
    public abstract Optional<String> userPrincipalName();

    @JsonProperty("department_name")
    public abstract Optional<String> departmentName();

    @JsonProperty("kind")
    public abstract String kind();

    @JsonProperty("manager_email")
    public abstract Optional<String> managerEmail();

    public abstract Optional<String> managerEmployeeId();

    public abstract Optional<String> title();

    @JsonProperty("mobile_phone")
    public abstract Optional<String> mobilePhone();

    @JsonProperty("office_phone")
    public abstract Optional<String> officePhone();

    @JsonProperty("organisational_unit_id")
    public abstract String organisationalUnitExternalId();

    public abstract Optional<Long> organisationalUnitId();





}
