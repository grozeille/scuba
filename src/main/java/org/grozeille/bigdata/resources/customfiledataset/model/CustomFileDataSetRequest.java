package org.grozeille.bigdata.resources.customfiledataset.model;

import lombok.Data;

@Data
public class CustomFileDataSetRequest {
    private String comment;

    private String[] tags;

    private Boolean temporary = false;
}
