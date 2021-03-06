package com.datagraphice.fcriscuo.alsdb.graphdb.service;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

import edu.jhu.fcriscu1.als.graphdb.value.DrugBankValue;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Maps;

public enum DrugBankService {
  INSTANCE;
  //TODO : use async logger
  private final Logger log = Logger.get(DrugBankService.class);
 private final Path drugLinksFilePath = Paths.get(com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE.getStringProperty("DRUG_LINKS_FILE"));
  private final ImmutableMap<String, DrugBankValue> drugBankMap =
      Suppliers.memoize(new DrugBankValueMapSupplier(drugLinksFilePath)).get();


  public Optional<DrugBankValue> getDrugBankValueById(@Nonnull String id) {
    if(drugBankMap.containsKey(id)) {
      return Optional.of(drugBankMap.get(id));
    }
    log.error(id +" is not a valid DrugBank identifier");
    return Optional.empty();
  }

  class DrugBankValueMapSupplier implements Supplier<ImmutableMap<String, DrugBankValue>>{
    private final Map<String,DrugBankValue> dbvMap = new HashMap<>();
    DrugBankValueMapSupplier(@Nonnull Path csvPath) {
      generateMap(csvPath);
    }

    private void generateMap(Path csvPath){
      new com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier(csvPath)
          .get()
          .map(DrugBankValue::parseCSVRecord)
          .forEach(dbv -> dbvMap.put(dbv.drugBankId(), dbv));
      log.info("The DrugBank Map conatins " +this.dbvMap.size() +" entries");
    }

    @Override
    public ImmutableMap<String, DrugBankValue> get() {
      return Maps.immutable.ofMap(this.dbvMap);
    }
  }

  public static void main(String[] args) {
    Arrays.asList("DB00141","DB00732", "DB09146","XXXXX")
        .forEach(id-> {
          DrugBankService.INSTANCE.getDrugBankValueById(id)
              .ifPresent(dbv -> System.out.println(dbv.drugBankId() +"  name: " +dbv.drugName()));
        });
  }
}
