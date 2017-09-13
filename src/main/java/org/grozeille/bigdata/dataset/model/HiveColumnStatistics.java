package org.grozeille.bigdata.dataset.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class HiveColumnStatistics {

    private String min;

    private String max;

    private String count;
}
