package org.nygenome.als.graphdb.util;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;


public enum FrameworkPropertyService {

    INSTANCE;
    private static final Logger log = Logger.getLogger(FrameworkPropertyService.class);
    private static final String PROPERTIES_FILE = "framework.properties";
    private final Configuration config = Suppliers.memoize(new ConfigurationSupplier()).get();

    /*
    Return an Optional of a Path specified in the properties file
     */
    public Optional<Path> getOptionalPathProperty(String propertyName) {
        if(config.containsKey(propertyName)) {
           return Optional.of(Paths.get(config.getString(propertyName)));
        }
        return Optional.empty();
    }

   public  String getStringProperty(String propertyName){
       if (config.containsKey(propertyName)){
           return config.getString(propertyName);
       } else {
           log.error(propertyName + " is not a valid proerty");
       }
       return "";
   }

    public  int getIntProperty(String propertyName){
        if (config.containsKey(propertyName)){
            return config.getInt(propertyName);
        } else {
            log.error(propertyName + " is not a valid proerty");
        }
        return -1;
    }

    public Optional<Path> getPathProperty(String propertyName){
        if (config.containsKey(propertyName)){
            return Optional.of(Paths.get(config.getString(propertyName)));
        }
        return Optional.empty();

    }

    class ConfigurationSupplier implements Supplier<Configuration> {
        private Configuration config;

        ConfigurationSupplier() {
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setFileName(PROPERTIES_FILE));
            try
            {
                this.config = builder.getConfiguration();
            }
            catch(ConfigurationException cex)
            {
               log.error(cex.getMessage());
               cex.printStackTrace();
            }
        }

        @Override
        public Configuration get() {
            return this.config;
        }
    }
// main method for stand alone testing
    public static void main(String... args) {
        // lookup a property
        String propertyName = "PPI_INTACT_FILE";
        String filePath = FrameworkPropertyService.INSTANCE.getStringProperty(propertyName);
        log.info("Property: " +propertyName +"  value:  "+filePath);
        // look up bad property - should get error message
        propertyName = "XXXXXXXXXX";
        String badValue =  FrameworkPropertyService.INSTANCE.getStringProperty(propertyName);
    }

}
