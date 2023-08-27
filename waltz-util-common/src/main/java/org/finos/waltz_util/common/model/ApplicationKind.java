package org.finos.waltz_util.common.model;

public enum ApplicationKind {
    /** Applications which have been developed by in-house software teams */
    IN_HOUSE,

    /** Deprecated, use THIRD_PARTY or CUSTOMISED instead **/
    @Deprecated
    INTERNALLY_HOSTED,

    /** Externally hosted applications such as Salesforce **/
    EXTERNALLY_HOSTED,

    /** End user computing - Applications not owned by IT **/
    EUC,

    /** Third party applications which have not been customised **/
    THIRD_PARTY,

    /** Third party applications which have been customised **/
    CUSTOMISED,

    /** Applications which are not owned by the organisation **/
    EXTERNAL
}
