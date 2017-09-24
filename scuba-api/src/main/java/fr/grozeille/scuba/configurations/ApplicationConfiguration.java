package fr.grozeille.scuba.configurations;

import fr.grozeille.scuba.ClusterConfiguration;
import fr.grozeille.scuba.configurations.stereotype.JpaRepository;
import fr.grozeille.scuba.configurations.stereotype.SolrRepository;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.repository.config.EnableSolrRepositories;
import org.springframework.data.solr.server.support.EmbeddedSolrServerFactory;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.SAXException;

import javax.annotation.Resource;
import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

//import org.springframework.security.oauth2.client.token.grant.code.AuthorizationCodeResourceDetails;

@EnableSolrRepositories(includeFilters = @ComponentScan.Filter(SolrRepository.class), basePackages = "fr.grozeille.scuba")
@EnableJpaRepositories(includeFilters = @ComponentScan.Filter(JpaRepository.class), basePackages = "fr.grozeille.scuba")
@Configuration
public class ApplicationConfiguration {

    @Resource
    private Environment environment;

    @Bean
    @ConfigurationProperties
    public ClusterConfiguration clusterConfiguration(){
        return new ClusterConfiguration();
    }

    @Bean
    public RestTemplate restTemplate(){
        //return new KerberosRestTemplate("/tmp/user.keytab", "user@EXAMPLE.ORG");
        return new RestTemplate();
    }

    @Bean
    public HiveMetaStoreClient hiveMetaStoreClient() throws MetaException {
        HiveConf hiveConf = new HiveConf();
        return new HiveMetaStoreClient(hiveConf);
    }

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        return FileSystem.get(conf);
    }

    @Bean
    @Primary
    @ConfigurationProperties(prefix="spring.datasource.primary")
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "hiveDataSource")
    @ConfigurationProperties(prefix="spring.datasource.hive")
    public DataSource hiveDataSource() {
        return DataSourceBuilder.create().build();
    }

    /*@Bean
    public HiveContext sparkContext() throws IOException, InterruptedException {

        //UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("vagrant", UserGroupInformation.getCurrentUser());
        //return proxyUser.doAs((PrivilegedExceptionAction<HiveContext>) () -> {

            SparkConf conf = new SparkConf()
                    .setAppName("hive-preparation-api")
                    //.setJars(new String[]{SparkContext.jarOfClass(this.getClass()).get()})
                    //.setJars(new String[]{ "hdfs:///user/mathias/spark-assembly-1.6.1.2.4.2.0-258-hadoop2.7.1.2.4.2.0-258.jar"})
                    .setMaster("yarn-client");

            conf.set("spark.yarn.jar", "hdfs:///user/mathias/spark-assembly-1.6.1.2.4.2.0-258-hadoop2.7.1.2.4.2.0-258.jar");
            conf.set("spark.driver.extraJavaOptions","-Dhdp.version=2.4.2.0-258");
            conf.set("spark.yarn.am.extraJavaOptions","-Dhdp.version=2.4.2.0-258");

            //conf.setSparkHome("/usr/hdp/current/spark-client/");

            JavaSparkContext sc = new JavaSparkContext(conf);
            HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

            return sqlContext;

        //});
    }*/


    @Bean
    public SolrClient solrServer() throws IOException, SAXException, ParserConfigurationException {
        ClusterConfiguration configuration = clusterConfiguration();
        if(configuration.getSolr().isEmbedded()) {
            EmbeddedSolrServerFactory factory = new EmbeddedSolrServerFactory(configuration.getSolr().getHome());
            return factory.getSolrClient();
        }
        else {
            return new CloudSolrClient(configuration.getSolr().getZkUrl());
        }
    }

    @Bean
    public SolrOperations solrTemplate() throws ParserConfigurationException, SAXException, IOException {
        return new SolrTemplate(solrServer());
    }
}
