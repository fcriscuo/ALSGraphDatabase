package com.datagraphice.fcriscuo.alsdb.graphdb.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.parboiled.common.Preconditions;

public class Utils {

  public static void deleteDirectoryAndChildren(Path path) throws IOException {
    // source: http://www.baeldung.com/java-delete-directory
    // validate that method parameter is a directory
    Preconditions.checkArgNotNull(path, "A Path is required");
    Preconditions.checkArgument(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS),
        path + " is not a directory");
    // delete the specified directory and all its child directories and files
    Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }

  /*
  Public function to parse the first line of a specified tsv file
  into an array of Strings
  Intended for resolving column headings
   */
  static public Function<Path, String[]> resolveColumnHeadingsFunction = (path) -> {
    List<String> headingList = new ArrayList<>();
    try {
      Files.lines(path)
          .findFirst()
          .ifPresent(line ->
              headingList.addAll(Arrays.asList(line.split("\t")
              )));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return headingList.toArray(new String[0]);
  };

}