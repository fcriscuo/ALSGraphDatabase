package edu.jhu.fcriscu1.als.graphdb.util;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import edu.jhu.fcriscu1.als.graphdb.poc.ReadResourceFileTest;
import org.apache.log4j.Logger;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;


public enum FrameworkPropertyService {

    /*
    mod 05Nov2018 FJC refactor to eliminate dependency upon Apache Commons Configuration API
     */

    INSTANCE;
    private static final Logger log = Logger.getLogger(FrameworkPropertyService.class);
    final static String PROPERTIES_FILE = "/framework.properties";
    private final ImmutableMap<String,String> propertiesMap =
            Suppliers.memoize(new PropertiesMapSupplier()).get();
/*
Public method to resolve a test file under the project's resources directory
 */
    public Optional<Path> getOptionalResourcePath(@Nonnull  String propertyName) {
        if(propertiesMap.containsKey(propertyName)) {
            try {
                URL resource =  ReadResourceFileTest.class.getResource(propertiesMap.get(propertyName));
                return Optional.of( Paths.get(resource.toURI()));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
            System.out.println("Property name " + propertyName + " is invalid");
            return Optional.empty();
    }

    /*
    Return an Optional of a Path specified in the properties file
     */
    public Optional<Path> getOptionalPathProperty(String propertyName) {
        if(propertiesMap.containsKey(propertyName)) {
            Path path = Paths.get(propertiesMap.get(propertyName));
            if(Files.isReadable(path)) {
                return Optional.of(path);
            }
        }
        System.out.println("Property name " + propertyName + " is invalid");
        return Optional.empty();
    }

   public  String getStringProperty(String propertyName){
       if(propertiesMap.containsKey(propertyName)) {
           return propertiesMap.get(propertyName);
       }
        else {
           log.error(propertyName + " is not a valid property");
       }
       return "";
   }

    public  int getIntProperty(String propertyName){
        if(propertiesMap.containsKey(propertyName)) {
            return Integer.valueOf(propertiesMap.get(propertyName));
        }
        else {
            log.error(propertyName + " is not a valid property");
        }
        return -1;
    }
/*
Inner class that will create a Map of property names & values from the defined
Properties file
 */
    class PropertiesMapSupplier implements Supplier<ImmutableMap<String,String>> {
        private MutableMap<String,String>  propertiesMap = Maps.mutable.empty();

        PropertiesMapSupplier(){
                this.resolvePropertiesMap();
        }
        private void resolvePropertiesMap(){
            try (InputStream stream = PropertiesMapSupplier.class.getResourceAsStream(FrameworkPropertyService.PROPERTIES_FILE);) {
                Properties p = new Properties();
                p.load(stream);
                Enumeration keys = p.propertyNames();
                while (keys.hasMoreElements()) {
                    String key = (String) keys.nextElement();
                    propertiesMap.put(key, p.getProperty(key));
                }
            } catch (IOException e ) {
                AsyncLoggingService.logError(e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public ImmutableMap<String, String> get() {
            return propertiesMap.toImmutable();
        }
    }

// main method for stand alone testing
    public static void main(String... args) {
        // lookup a file sspecified in the properties file
        String propertyName = "PPI_INTACT_FILE";
        String filePath = FrameworkPropertyService.INSTANCE.getStringProperty(propertyName);
       AsyncLoggingService.logInfo("Property: " +propertyName +"  value:  "+filePath);
       // look up a resource file
        propertyName = "TEST_PROACT_ADVERSE_EVENT_FILE";
        FrameworkPropertyService.INSTANCE.getOptionalResourcePath(propertyName)
                .ifPresent(path -> System.out.println("Resource path = " +path.toString()));
        // look up bad property - should get error message
        propertyName = "XXXXXXXXXX";
        String badValue =  FrameworkPropertyService.INSTANCE.getStringProperty(propertyName);
    }

}
