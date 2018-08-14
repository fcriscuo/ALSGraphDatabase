package org.nygenome.als.graphdb.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.parboiled.common.Preconditions;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;

public class Utils {

	public static void deleteDirectoryAndChildren(Path path) throws IOException {
		// source: http://www.baeldung.com/java-delete-directory
		// validate that method parameter is a directory
		Preconditions.checkArgNotNull(path, "A Path is required");
		Preconditions.checkArgument(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS),path +" is not a directory");
		// delete the specified directory and all its child directories and files
		Files.walk(path)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
	}



	// return the relationship type based on the first element of the String
	static public RelTypes convertStringListToRelType (List<String> stringList) {
		return (stringList.size()>0) ? convertStringToRelType(stringList.get(0))
				: RelTypes.eNoEvent;
	}

	static public RelTypes convertStringToRelType(String string) {

		if (string.toUpperCase().trim().equals("BIOMARKER"))
			return RelTypes.BIOMARKER;

		if (string.toUpperCase().trim().equals("THERAPEUTIC"))
			return RelTypes.THERAPEUTIC;

		if (string.toUpperCase().trim().equals("GENETICVARIATION"))
			return RelTypes.GENETIC_VARIATION;
			
		
		if ((string.toUpperCase().trim().contains("PHYSICAL ASSOCIATION"))
				|| (string.toUpperCase().trim().contains("ASSOCIATION"))
				|| (string.toUpperCase().trim().contains("DIRECT INTERACTION"))
				|| (string.toUpperCase().trim().contains("COVALENT BINDING"))
				|| (string.toUpperCase().trim().contains("ENZYMATIC REACTION"))
				|| (string.toUpperCase().trim().contains("SELF INTERACTION"))
				|| (string.toUpperCase().trim().contains("DISULFIDE BOND"))
				|| (string.toUpperCase().trim().contains("TRANSGLUTAMINATION REACTION"))
				|| (string.toUpperCase().trim().contains("PHOSPHORYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("SUMOYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("CLEAVAGE REACTION"))
				|| (string.toUpperCase().trim().contains("DEPHOSPHORYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("UBIQUITINATION REACTION"))
				|| (string.toUpperCase().trim().contains("OXIDOREDUCTASE ACTIVITY ELECTRON TRANSFER REACTION"))
				|| (string.toUpperCase().trim().contains("ACETYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("DEUBIQUITINATION REACTION"))
				|| (string.toUpperCase().trim().contains("GTPASE REACTION"))
				|| (string.toUpperCase().trim().contains("PROTEIN CLEAVAGE"))				
				|| (string.toUpperCase().trim().contains("DEACETYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("METHYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("PALMITOYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("NEDDYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("HYDROXYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("ADP RIBOSYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("PROLINE ISOMERIZATION  REACTION"))
				|| (string.toUpperCase().trim().contains("PHOSPHOTRANSFER REACTION"))
				|| (string.toUpperCase().trim().contains("DEMETHYLATION REACTION"))
				|| (string.toUpperCase().trim().contains("RNA CLEAVAGE"))
				|| (string.toUpperCase().trim().contains("DENEDDYLATION REACTION")))
			return RelTypes.PPI_ASSOCIATION;

		if (string.toUpperCase().trim().contains("COLOCALIZATION"))
			return RelTypes.PPI_COLOCALIZATION;

		if (string.toUpperCase().trim().contains("GENETIC INTERACTION"))
			return RelTypes.PPI_GENETIC_INTERACTION;

		if (string.toUpperCase().trim().contains("PREDICTED INTERACTION"))
			return RelTypes.PPI_PREDICTED_INTERACTION;

		return RelTypes.eNoEvent;
	}
}
