package org.grozeille.bigdata.dataset.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomFileDataSetFileStream {
    private CustomFileDataSetFileInfo fileInfo;

    private InputStream inputStream;
}
