
# ALSGraphDatabase

Java/Scala code to support the development of a Neo4j graph database 
persisting genomic and clinical data relating to ALS. This project is
based on the ProteinFramework code (https://github.com/ibalaur/ProteinFramework)
which is part of the DiseaseNetworks module from the eTRIKS Lab.[Lysenko]
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

Ensembl mapping: http://useast.ensembl.org/biomart/martview/b07cb8bd396d9e4434ef2d90860ba72b
                 http://useast.ensembl.org/biomart/martview/1388e394c67e8eec6f7382a996675b52

 The ALS Online Genetics Database (ALSoD) : http://alsod.iop.kcl.ac.uk/home.aspx
 Olubunmi Abel, Aleksey Shatunov, Ashley R Jones, Peter M Andersen, John F Powell, Ammar Al-Chalabi 
   "Development of a Smartphone App for a Genetics Website: The Amyotrophic Lateral Sclerosis Online 
   Genetics Database (ALSoD)." 
   JMIR Mhealth Uhealth 2013;1(2):e18 doi: 10.2196/mhealth.2706
   
References:
Lysenko, A, et al. "Representing and querying disease networks using graph databases", BioData Mining,
2016, 9:23 ,https://doi.org/10.1186/s13040-016-0102-8

 Olubunmi Abel, Aleksey Shatunov, Ashley R Jones, Peter M Andersen, John F Powell, Ammar Al-Chalabi 
   "Development of a Smartphone App for a Genetics Website: The Amyotrophic Lateral Sclerosis Online 
   Genetics Database (ALSoD)." 
   JMIR Mhealth Uhealth 2013;1(2):e18 doi: 10.2196/mhealth.2706

