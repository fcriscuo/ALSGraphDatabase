
# ALSGraphDatabase

Java/Scala code to support the development of a Neo4j graph database 
persisting genomic and clinical data relating to ALS. This project is
a based on the ProteinFramework code (https://github.com/ibalaur/ProteinFramework)
which is part of the DiseaseNetworks module from the eTRIKS Lab.
The intention of this project is to modify and extend the original project to 
be specific for ALS data. 

Neo4j Nodes and Relationships are created by a set of Java classes that implement the
Java Consumer interface. Each Consumer processes a specific type of data (e.g. protein,
pathway, disease, etc.) contained in tab or comma delimited files. The appropriate 
filenames are registered in the frameworks.properties file. Each Consumer has a main
method to support standalone testing. In this mode, a test graph database will be generated.
Standalone execution references an abbreviated test input file in those cases where the
source data are too large. While the individual Consumers may be invoked in any order,
it is best to invoke the UniProtValueConsumer first to complete creation of the Protein
Nodes.

Data Sources:
DrugBank: https://www.drugbank.ca/releases/latest