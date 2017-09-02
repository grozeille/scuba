package org.grozeille.bigdata.resources.dataset;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.repositories.solr.DataSetRepository;
import org.grozeille.bigdata.services.CatalogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class DataSetResource {

    @Autowired
    private DataSetRepository dataSetRepository;

    @Autowired
    private CatalogService catalogService;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<DataSet> dataset(Pageable pageable, @RequestParam(value = "filter", required = false, defaultValue = "") String filter) {
        if(Strings.isNullOrEmpty(filter)) {
            return dataSetRepository.findAll(pageable);
        }
        else {
            return dataSetRepository.findByAll(pageable, filter);
        }
    }


    @RequestMapping(value = "/refresh", method = RequestMethod.POST)
    public void refresh() throws Exception {

        catalogService.refreshCatalog();

    }
}
