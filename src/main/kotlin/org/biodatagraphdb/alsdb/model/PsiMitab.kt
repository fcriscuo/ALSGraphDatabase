package org.biodatagraphdb.alsdb.model

import org.apache.commons.csv.CSVRecord

data class PsiMitab(
        val interactorAId: String, val interactorBId: String, val altIdAList: List<String>,
        val altIdBList: List<String>, val aliasAList: List<String>, val aliasBList: List<String>,
        val detectionMethodList: List<String>, val firstAuthorList: List<String>,
        val publicationIdList: List<String>, val taxonmyAList: List<String>,
        val taxonmyBList: List<String>, val interactionTypeList: List<String>,
        val sourceDatabaseList: List<String>, val interactionIdentifierList: List<String>,
        val confidenceValuesList: List<String>, val expansionMethodsList: List<String>,
        val biologicalRoleAList: List<String>, val biologicalRoleBList: List<String>,
        val experimentalRoleAList: List<String>,
        val experimentalRoleBList: List<String>,
        val typesAList: List<String>, val typesBList: List<String>,
        val xrefAList: List<String>,
        val xrefBList: List<String>,
        val xrefInteractionList: List<String>,
        val annotationAList: List<String>,
        val annotationBList: List<String>,
        val annotationInteractionList: List<String>,
        val hostOrganismList: List<String>,
        val interactionParameterList: List<String>, val negative: Boolean,
        val featureListA: List<String>, val featureListB: List<String>
) {
/*
#uidA	uidB	altA	altB	aliasA	aliasB	method	author
pmids	taxa	taxb	interactionType	sourcedb
interactionIdentifier	confidence	expansion
biological_role_A	biological_role_B	experimental_role_A
experimental_role_B	interactor_type_A	interactor_type_B
xrefs_A	xrefs_B	xrefs_Interaction	Annotations_A	Annotations_B
Annotations_Interaction	Host_organism_taxid
parameters_Interaction	Creation_date	Update_date	Checksum_A
Checksum_B	Checksum_Interaction	Negative	OriginalReferenceA
OriginalReferenceB	FinalReferenceA	FinalReferenceB	MappingScoreA
MappingScoreB	irogida	irogidb	irigid	crogida	crogidb	crigid
icrogida	icrogidb	icrigid	imex_id	edgetype	numParticipants
 */
    companion object : AlsdbModel {
        val INTACT_HEADER_STRING: String = listOf("#ID(s) interactor A",
                "ID(s) interactor B",
                "Alt. ID(s) interactor A",
                "Alt. ID(s) interactor B",
                "Alias(es) interactor A",
                "Alias(es) interactor B",
                "Interaction detection method(s)",
                "Publication 1st author(s)",
                "Publication Identifier(s)",
                "Taxid interactor A",
                "Taxid interactor B",
                "Interaction type(s)",
                "Source database(s)",
                "Interaction identifier(s)",
                "Confidence value(s)",
                "Expansion method(s)",
                "Biological role(s) interactor A",
                "Biological role(s) interactor B",
                "Experimental role(s) interactor A",
                "Experimental role(s) interactor B",
                "Type(s) interactor A",
                "Type(s) interactor B",
                "Xref(s) interactor A",
                "Xref(s) interactor B",
                "Interaction Xref(s)", "",
                "Annotation(s) interactor A",
                "Annotation(s) interactor B",
                "Interaction annotation(s)",
                "Host organism(s)",
                "Interaction parameter(s)",
                "Creation date",
                "Update date",
                "Checksum(s) interactor A",
                "Checksum(s) interactor B",
                "Interaction Checksum(s)",
                "Negative",
                "Feature(s) interactor A",
                "Feature(s) interactor B", "",
                "Stoichiometry(s) interactor A",
                "Stoichiometry(s) interactor B",
                "Identification method participant A",
                "Identification method participant B").joinToString(separator = "\t")

        fun parseCSVRecord(record: CSVRecord): PsiMitab =
                PsiMitab(
                        parseStringOnColon(record.get("#ID(s) interactor A")).last(),
                        parseStringOnColon(record.get("ID(s) interactor B")).last(),
                        parseStringOnPipe(record.get("Alt. ID(s) interactor A")),
                        parseStringOnPipe(record.get("Alt. ID(s) interactor B")),
                        parseStringOnPipe(record.get("Alias(es) interactor A")),
                        parseStringOnPipe(record.get("Alias(es) interactor B")),
                        parseStringOnPipe(record.get("Interaction detection method(s)")),
                        parseStringOnPipe(record.get("Publication 1st author(s)")),
                        parseStringOnPipe(record.get("Publication Identifier(s)")),
                        parseStringOnPipe(record.get("Taxid interactor A")),
                        parseStringOnPipe(record.get("Taxid interactor B")),
                        parseStringOnPipe(record.get("Interaction type(s)")),
                        parseStringOnPipe(record.get("Source database(s)")),
                        parseStringOnPipe(record.get("Interaction identifier(s)")),
                        parseStringOnColon(record.get("Confidence value(s)")),
                        parseStringOnPipe(record.get("Expansion method(s)")),
                        parseStringOnPipe(record.get("Biological role(s) interactor A")),
                        parseStringOnPipe(record.get("Biological role(s) interactor B")),
                        parseStringOnPipe(record.get("Experimental role(s) interactor A")),
                        parseStringOnPipe(record.get("Experimental role(s) interactor B")),
                        parseStringOnPipe(record.get("Type(s) interactor A")),
                        parseStringOnPipe(record.get("Type(s) interactor B")),
                        parseStringOnPipe(record.get("Xref(s) interactor A")),
                        parseStringOnPipe(record.get("Xref(s) interactor B")),
                        parseStringOnColon(record.get("Interaction Xref(s)")),
                        parseStringOnPipe(record.get("Annotation(s) interactor A")),
                        parseStringOnPipe(record.get("Annotation(s) interactor B")),
                        parseStringOnPipe(record.get("Interaction annotation(s)")),
                        parseStringOnPipe(record.get("Host organism(s)")),
                        parseStringOnPipe(record.get("Interaction parameter(s)")),
                        record.get("Negative").toBoolean(),
                        parseStringOnPipe(record.get("Feature(s) interactor A")),
                        parseStringOnPipe(record.get("Feature(s) interactor B"))
                )
    }
}