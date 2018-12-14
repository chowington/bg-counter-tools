ONTOLOGY SOURCE REFERENCE
Term Source Name,MO,EFO,OBI,UO,GAZ,NCBITaxon,PATO,MIRO,efo,IDOMAL,SO,VBcv
Term Source File,http://mged.sourceforge.net/ontologies/MGEDontology.php
Term Source Version,1.3.0.1,http://data.bioontology.org/ontologies/EFO,47893,49121,46911,47845,47893,49589,49584,47619,49414,v0.50
Term Source Description,The Microarray Ontology,Experimental Factor Ontology,Ontology for Biomedical Investigations,Units of measurement,Gazetteer,NCBI organismal classification,Ontology for Biomedical Investigations,Mosquito insecticide resistance,Experimental Factor Ontology,Malaria Ontology,Sequence types and features,VectorBase controlled vocabulary

STUDY
Study Identifier,${year}-${month}-${prefix}-smart-trap-surveillance
Study Title,Mosquito surveillance from ${start_date} to ${end_date} by BG-Counter smart traps owned by ${org_name}.
Study Submission Date,${year}
Study Public Release Date,${year}
Study Description,Mosquito surveillance from ${start_date} to ${end_date} by BG-Counter smart traps owned by ${org_name}.
Study File Name,s_samples.txt

# Project tags: must be children of VBcv:0001076 'project tag'
STUDY TAGS
Study Tag,abundance,${study_tag}
Study Tag Term Accession Number,0001085,${study_tag_number}
Study Tag Term Source Ref,VBcv,VBcv

STUDY DESIGN DESCRIPTORS
Study Design Type,observational design,strain or line design
Study Design Type Term Accession Number,0000629,0001754
Study Design Type Term Source Ref,EFO,EFO

STUDY PUBLICATIONS
Study PubMed ID
Study Publication DOI
Study Publication Author list,${org_name}
Study Publication Title,Mosquito surveillance from ${start_date} to ${end_date} by BG-Counter smart traps owned by ${org_name}.
Study Publication Status,unpublished
Study Publication Status Term Accession Number,0000233
Study Publication Status Term Source Ref,VBcv
Comment[URL],${org_url}

# VectorBase is currently not planning to use factors and factor values in popbio ISA-Tab submissions.
"# Instead we ask you to provide our curators with a description of the pertinent Characteristics, Parameter Values, Genotypes and/or Phenotypes to be used in the display of the results."
STUDY FACTORS
Study Factor Name
Study Factor Type
Study Factor Type Term Accession Number
Study Factor Type Term Source Ref

STUDY ASSAYS
Study Assay Measurement Type,field collection,species identification assay
Study Assay Measurement Type Term Accession Number
Study Assay Measurement Type Term Source Ref
Study Assay Technology Type
Study Assay Technology Type Term Accession Number
Study Assay Technology Type Term Source Ref
Study Assay Technology Platform
Study Assay File Name,a_collection.txt,a_species.txt

STUDY PROTOCOLS
Study Protocol Name,COLLECT_BGCT,SPECIES
Study Protocol Type,BG-Counter trap catch,by size
Study Protocol Type Term Accession Number,0000143,0000145
Study Protocol Type Term Source Ref,IRO,IRO
Study Protocol Description,Automatic smart trap with CO2 attractant,Identification of mosquitoes by size.
Study Protocol URI,https://www.bg-counter.com/
Study Protocol Version
Study Protocol Parameters Name
Study Protocol Parameters Name Term Accession Number
Study Protocol Parameters Name Term Source Ref
Study Protocol Components Name
Study Protocol Components Type
Study Protocol Components Type Term Accession Number
Study Protocol Components Type Term Source Ref






STUDY CONTACTS
Study Person Last Name,${contact_last_name}
Study Person First Name,${contact_first_name}
Study Person Mid Initials
Study Person Email,${contact_email}
Study Person Phone
Study Person Fax
Study Person Address
Study Person Affiliation,${org_name}
Study Person Roles,investigation agent role
Study Person Roles Term Accession Number,0000122
Study Person Roles Term Source Ref,OBI

# ISA-Tab allows multiple studies (a new STUDY section following this line) but for VectorBase submissions we only accept single studies at the moment.