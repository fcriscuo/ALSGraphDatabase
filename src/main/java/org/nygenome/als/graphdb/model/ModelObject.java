package org.nygenome.als.graphdb.model;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public abstract class ModelObject {

  private static final String HUMAN_SPECIES = "Homo sapiens";

  protected static Function<String,List<String>> parseStringOnPipeFunction = (s)->
      Arrays.asList( s.split(Pattern.quote("|")));

  protected static Function<String,List<String>> parseStringOnColonFunction = (s)->
      Arrays.asList( s.split(Pattern.quote(":")));

  protected static Function<String,List<String>> parseStringOnSemiColonFunction = (s)->
      Arrays.asList( s.split(Pattern.quote(";")));

  public static Predicate<String> humanSpeciesPredicate = (species)->
           species.equals(HUMAN_SPECIES);
}
