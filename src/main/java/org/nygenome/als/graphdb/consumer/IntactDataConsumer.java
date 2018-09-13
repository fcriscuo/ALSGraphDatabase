package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;

//import org.nygenome.als.graphdb.model.PsiMitab;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.value.PsiMitab;
import scala.Tuple2;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class IntactDataConsumer extends GraphDataConsumer implements Consumer<Path> {

  private static final String HEADINGS_FILE_NAME = "heading_intact.txt";
  private static final String INTACT_FILE_PREFIX = "intact.txt";

  private Predicate<PsiMitab> selfInteractionPredicate = (ppi) ->
      ! ppi.intearctorAId().equals(ppi.interactorBId());


  private Function<Path,Path> resolveHeadingFilePathFunction = (dirPath) ->
    Paths.get(dirPath.toString(),HEADINGS_FILE_NAME);

  /*
  Process the human intact data. The file provided from IntAct is excessively
  large. Manually split the large file using the split command and process all
  the individual components
  The Path argument must be a directory
   public TsvRecordStreamSupplier(@Nonnull Path aPath,
        @Nonnull String...columnHeadings)
   */
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS),
    path.toString() +" is not a directory");
    String[] columnHeadings = Utils.resolveColumnHeadingsFunction
        .apply(resolveHeadingFilePathFunction.apply(path));
    try {
      Files.walk(path)
          .filter(filePath -> filePath.getFileName().toString().startsWith(INTACT_FILE_PREFIX))
          .forEach(filePath -> {
            System.out.println("Processing intact file: " +filePath.toString());
            new TsvRecordStreamSupplier(filePath,
                columnHeadings).get()
                .map(PsiMitab::parseCSVRecord)
                .filter(selfInteractionPredicate)
                .forEach(proteinInteractionConsumer);
          } );
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Consumer<Tuple2<String,String>> novelProteinNodesConsumer = (tuple) -> {
    if (!proteinMap.containsKey(tuple._1())) {
      createProteinNode(strNoInfo, tuple._1(), strNoInfo,
          strNoInfo, strNoInfo, strNoInfo);
    }
    if (!proteinMap.containsKey(tuple._2())) {
      createProteinNode(strNoInfo,tuple._2(), strNoInfo,
          strNoInfo, strNoInfo, strNoInfo);
    }
  };

  private Consumer<PsiMitab> proteinInteractionConsumer = (ppi) -> {
    Tuple2<String,String> abTuple = new Tuple2<>(ppi.intearctorAId(),ppi.interactorBId());
    Tuple2<String,String> baTuple = new Tuple2<>(ppi.interactorBId(), ppi.intearctorAId());
    if ((!vPPIMap.containsKey(abTuple))
        && (!vPPIMap.containsKey(baTuple))) {
      novelProteinNodesConsumer.accept(abTuple);  // register protein node if novel
      vPPIMap.put(
          abTuple,
          proteinMap
              .get(ppi.intearctorAId())
              .createRelationshipTo(
                  proteinMap.get(ppi.interactorBId()),
                  Utils.convertStringToRelType (ppi.interactionTypeList().head())));
      // add interaction properties
      // detection methods
      vPPIMap.get(abTuple).setProperty("Interaction_method_detection",
         ppi.detectionMethodList().mkString("|"));

      vPPIMap.get(abTuple).setProperty("References",
          ppi.publicationIdList().mkString("|"));

      if (null != ppi.confidenceScoreList() && ppi.confidenceScoreList().size() >0) {
        vPPIMap.get(abTuple).setProperty("Confidence_level",
            Double.parseDouble(ppi.confidenceScoreList().head()));
      }
    }

  };


}

