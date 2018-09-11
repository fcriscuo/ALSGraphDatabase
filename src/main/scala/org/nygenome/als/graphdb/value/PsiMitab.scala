package org.nygenome.als.graphdb.value


import org.apache.commons.csv.CSVRecord
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


case class PsiMitab(intearctorAId: String, interactorBId: String, altIdAList: List[String],
                    altIdBList: List[String], aliasAList: List[String], aliasBList: List[String],
                    detectionMethodList: List[String], firstAuthorList: List[String],
                    publicationIdList: List[String], taxonmyAList: List[String],
                    taxonmyBList: List[String], interactionTypeList: List[String],
                    sourceDtabaseList: List[String], databaseIdentifierList: List[String],
                    confidenceScoreList: List[String], complexExpansionList: List[String],
                    biologicalRoleAList: List[String], biologicalRoleBList: List[String],
                    experimentalRoleAList: List[String],
                    experimentalRoleBList: List[String], xrefAList: List[String],
                    xrefBList: List[String],
                    typeAList: List[String], typeBList: List[String], xrefInteractionList:
                    List[String], annotationAList: List[String],
                    annotationBList: List[String], annotationInteractionList: List[String],
                    hostTaxonomyList: List[String],
                    interactionParameterList: List[String], negative: Boolean,
                    featureListA: List[String], featureListB: List[String]
                   ) {
}

object PsiMitab extends ValueTrait{
  private val INTACT_HEADER_STRING = "#ID(s) interactor A\tID(s) interactor B\tAlt. ID(s) interactor A" + "\tAlt. ID(s) interactor B\tAlias(es) interactor A\tAlias(es) interactor B" + "\tInteraction detection method(s)\tPublication 1st author(s)\tPublication Identifier(s)" + "\tTaxid interactor A\tTaxid interactor B\tInteraction type(s)\tSource database(s)" + "\tInteraction identifier(s)\tConfidence value(s)\tExpansion method(s)\tBiological role(s) interactor A" + "\tBiological role(s) interactor B\tExperimental role(s) interactor A\tExperimental role(s) interactor B" + "\tType(s) interactor A\tType(s) interactor B\tXref(s) interactor A\tXref(s) interactor B\tInteraction Xref(s)" + "\tAnnotation(s) interactor A\tAnnotation(s) interactor B\tInteraction annotation(s)\tHost organism(s)" + "\tInteraction parameter(s)\tCreation date\tUpdate date\tChecksum(s) interactor A\tChecksum(s) interactor B" + "\tInteraction Checksum(s)\tNegative\tFeature(s) interactor A\tFeature(s) interactor B" + "\tStoichiometry(s) interactor A\tStoichiometry(s) interactor B\tIdentification method participant A" + "\tIdentification method participant B"

  def parseCSVRecord (record:CSVRecord):PsiMitab = {
    new PsiMitab(
      record.get("#ID(s) interactor A"),
      record.get("ID(s) interactor B"),
     parseStringOnPipeFunction(record.get("Alt. ID(s) interactor A")),
      parseStringOnPipeFunction(record.get("Alt. ID(s) interactor B")),
    parseStringOnPipeFunction(record.get("Alias(es) interactor A")),
      parseStringOnPipeFunction(record.get("Alias(es) interactor B")),
    parseStringOnPipeFunction(record.get("Interaction detection method(s)")),
    parseStringOnPipeFunction(record.get("Publication 1st author(s)")),
    parseStringOnPipeFunction(record.get("Publication Identifier(s)")),
    parseStringOnPipeFunction(record.get("Taxid interactor A")),
    parseStringOnPipeFunction(record.get("Taxid interactor B")),
    parseStringOnPipeFunction(record.get("Source database(s)")),
    parseStringOnPipeFunction(record.get("Interaction identifier(s)")),
    parseStringOnPipeFunction(record.get("Interaction type(s)")),
    parseStringOnPipeFunction(record.get("Biological role(s) interactor A")),
    parseStringOnPipeFunction(record.get("Biological role(s) interactor B")),
    parseStringOnPipeFunction(record.get("Experimental role(s) interactor A")),
    parseStringOnPipeFunction(record.get("Experimental role(s) interactor B")),
    parseStringOnPipeFunction(record.get("Experimental role(s) interactor A")),
    parseStringOnPipeFunction(record.get("Experimental role(s) interactor B")),
    parseStringOnPipeFunction(record.get("Type(s) interactor A")),
    parseStringOnPipeFunction(record.get("Type(s) interactor B")),
    parseStringOnPipeFunction(record.get("Xref(s) interactor A")),
    parseStringOnPipeFunction(record.get("Xref(s) interactor B")),
    parseStringOnColonFunction(record.get("Interaction Xref(s)")),
    parseStringOnPipeFunction(record.get("Annotation(s) interactor A")),
    parseStringOnPipeFunction(record.get("Annotation(s) interactor B")),
    parseStringOnPipeFunction(record.get("Interaction annotation(s)")),
    parseStringOnPipeFunction(record.get("Host organism(s)")),
    parseStringOnPipeFunction(record.get("Interaction parameter(s)")),
    record.get("Negative").toBoolean,
    parseStringOnPipeFunction(record.get("Feature(s) interactor A")),
    parseStringOnPipeFunction(record.get("Feature(s) interactor B"))


    )
  }



}
