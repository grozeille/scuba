package org.grozeille.bigdata.dataset.model;

import lombok.Data;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.SolrDocument;

@Data
@SolrDocument
public class DataSetSearchItem {

    @Id
    @Field
    private String id;

    @Getter
    //@Indexed(stored = false, readonly = true, searchable = true)
    @Field
    private String text;

    //@Indexed(copyTo = "all", boost = 1.0f)
    @Field
    private String database;

    //@Indexed(copyTo = "all", boost = 1.0f)
    @Field
    private String table;

    //@Indexed(copyTo = "all")
    @Field
    private String path;

    //@Indexed(copyTo = "all")
    @Field
    private String comment;

    //@Indexed(copyTo = "all")
    @Field
    private String creator;

    //@Indexed(copyTo = "all")
    @Field
    private String[] tags;

    //@Indexed(copyTo = "all")
    @Field
    private String format;

    //@Indexed(copyTo = "all")
    @Field
    private String[] columns;

    //@Indexed(copyTo = "all")
    @Field
    private String[] columnsComment;

    @Field
    private String dataSetType;

    //@Indexed(stored = true, searchable = false)
    @Field
    private String jsonData;

}
