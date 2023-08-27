package org.finos.waltz_util.loader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.finos.waltz_util.common.model.ApplicationKind;
import org.finos.waltz_util.common.model.Criticality;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
@JsonSerialize(as = ImmutableApplicationOverview.class)
@JsonDeserialize(as = ImmutableApplicationOverview.class)
public abstract class ApplicationOverview {

    @JsonProperty("external_id")
    public abstract String externalId();

    @Value.Default
    public ApplicationKind kind() {
        return ApplicationKind.IN_HOUSE;
    }

    public abstract String name();

    @Value.Default
    public String provenance() {
        return "waltz-loader";
    }

    public abstract Optional<String> description();

    @Value.Default
    @JsonProperty("lifecycle_phase")
    public String lifecyclePhase() {
        return "ACTIVE";

    }


    @JsonProperty("organisational_unit_id")
    public abstract String organisationalUnitExternalId();


    @Value.Default
    public Criticality criticality() {
        return Criticality.MEDIUM;
    }

    @Value.Auxiliary
    public abstract Optional<Long> id();


    public abstract Optional<Long> orgUnitId();

}
