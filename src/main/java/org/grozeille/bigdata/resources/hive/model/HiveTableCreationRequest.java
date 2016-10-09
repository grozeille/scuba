package org.grozeille.bigdata.resources.hive.model;

import lombok.Data;

@Data
public class HiveTableCreationRequest {
    private String dataDomainOwner;

    private String comment;

    private String[] tags;
}
