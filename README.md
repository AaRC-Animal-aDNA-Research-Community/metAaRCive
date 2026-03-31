## **metAaRCive: Overview**

metAaRCive is a resource of metadata for published ancient animal genomes, curated by members of [AaRC](https://animal-adna.org/). The resource is distributed as a tab-delimited text file, with each row corresponding to a single genome/sample/individual. The individual-sheets folder holds the same rows, but split across individual files corresponding to rough taxonomical groupings of organisms.

The folder raw holds a version of the resource that includes entries that did not pass validation, representing work-in-progress and will contain many formatting errors. The raw version is not recommended for most users, but is provided in case it might be useful to someone.

## **Get involved**

To contribute to the resource, join our Element channel (see https://animal-adna.org/about/) and find the metadata working group.



## **Summary of curated metadata**

| Sheet | Entries released | Nuclear data | Mitochondrial data | Papers | Entries raw |
|---|---|---|---|---|---|
| bos | 127 | 127 | 127 | 7 | 127 |
| beluga | 0 | 0 | 0 | 0 | 53 |
| hippos | 2 | 2 | 2 | 2 | 2 |
| land_snails | 2 | 2 | 2 | 2 | 2 |
| canids | 283 | 283 | 12 | 32 | 287 |
| ovis | 206 | 149 | 126 | 8 | 667 |
| capra | 145 | 145 | 79 | 11 | 145 |
| ursus | 0 | 0 | 0 | 0 | 50 |
| elephantidae | 50 | 50 | 50 | 7 | 311 |
| cervinae | 0 | 0 | 0 | 0 | 72 |
| sus | 354 | 354 | 0 | 4 | 354 |
| felinae | 172 | 172 | 1 | 7 | 172 |
| rodent | 52 | 39 | 52 | 2 | 52 |
| nh_primates | 15 | 4 | 15 | 8 | 15 |
| columbidae | 97 | 97 | 0 | 5 | 97 |
| rhinocerotidae | 0 | 0 | 0 | 0 | 72 |
| sirenia | 13 | 13 | 13 | 3 | 13 |
| sturgeon | 0 | 0 | 0 | 0 | 0 |
| Summary | 1518 | 1437 | 479 | 98 | 2491 |



## **Field definitions**

| Field | Description |
|---|---|
| samp_name | Sample name, the primary ID used in the DNA paper |
| source_mat_id | Any archaeology/museum ID(s) that exist for the sample |
| sample_alt_lab_ids | Optional: Any other sample aliases or IDs |
| biosamples_accession | BioSamples accession(s), can be more than one if incorrectly registered multiple times |
| tissue_type | Sampled tissue or element (Uberon ontology: https://www.ebi.ac.uk/ols4/ontologies/uberon) |
| molecular_sex | Genetically inferred sex |
| molecular_sex_reference | Reference/publication for molecular sex inference. If an unpublished inference: "AaRC curator" |
| samp_taxon_common | Sample taxon common name |
| samp_taxon_ID | NCBI taxonomy ID (https://www.ncbi.nlm.nih.gov/taxonomy), if one exists for the species |
| specific_host | The taxonomic (Latin) name of the host from which the tissue/DNA originated, to as low a level as possible. |
| latitude | Latitude where the sample was found |
| longitude | Longitude where the sample was found |
| geo_loc_name | geographic location (country and/or sea,region) |
| site_name | Name of archaelogical or natural site where remains were found. If there is no relevant site name, this can also be some geographical label, e.g. an island or village. If there are multiple names (including in multiple languages), can indicate these separated by "/" |
| sample_age | Point estimate, cal BP radiocarbon date if available, or other point estimate, relative to an age 0 at the year 1950. This should be a number directly usable in analyses. If a sample is more recent than 1950, give a negative number. If only a range is available, use the midpoint of the range. |
| sample_age_upper | Upper (older) end of date range |
| sample_age_lower | Lower (younger) end of date range |
| sample_age_inference_methods | The method used to infer the sample age |
| c14_age | Uncalibrated C14 age, BP |
| c14_age_sd | C14 standard deviation |
| c14_lab_code | C14 lab code |
| sample_age_reference | Reference/publication for age information |
| sample_age_notes | Optional: any potentially useful notes on the age of the samples |
| nuclear_sequencing_platform | What sequencing technology was used to generate nuclear data |
| nuclear_library_strategy | What library strategy was used to generate nuclear data, e.g. WGS or Targeted-Capture? |
| nuclear_depth_of_coverage | Depth of sequencing coverage of the nuclear genome, if shotgun sequencing |
| nuclear_damage_treatment | Indication of whether characteristic ancient DNA damage has been enzymatically or chemically removed in a laboratory |
| nuclear_lib_strandedness | The strandedness of the original template DNA molecules used for constructing the sequencing library |
| nuclear_reference | Reference/publication for nuclear genomic data |
| mt_sequencing_platform | What sequencing technology was used to generate mitochondrial data |
| mt_library_strategy | What library strategy was used to generate mitochondrial data, e.g. Targeted-Capture or AMPLICON (for PCR-based experiments). If there is a mitochondrial sequence obtained from WGS data, and that sequence is deposited with an accession number recorded in the field "mt_accession", then use "WGS" here in the "mt_library_strategy" field |
| mt_targeted_region | What region of the mitochondrial genome was targetted? |
| mt_depth_of_coverage | Depth of sequencing coverage of the mitochondrial genome |
| mt_damage_treatment | Indication of whether characteristic ancient DNA damage has been enzymatically or chemically removed in a laboratory |
| mt_lib_strandedness | The strandedness of the original template DNA molecules used for constructing the sequencing library |
| mt_accession | Accession number for assembled mitochondrial data (not raw reads) |
| mt_reference | Reference for mitochondrial data |
| curator_comments | Optional: Any general comments from the curator  |
| curated_by | Listing of the people that curated this record, can be multiple. |
| curation_complete | Is this curation of this record complete? |



## **Contributors**

Anders Bergström, Dani Kitaygorodskiy, Deon de Jager, George Popovici, Germán Hernández-Alonso, Gisela Kopp, Hannah Moots, He Yu, Jolijn Erven, Juliana Larsdotter, Kevin Daly, Lachie Scarsbrook, Laura Viñas Caron, Lohit Garikipati, Marco De Martino, Marianne Dehasque, Mattias Sherman, Nikolaos Psonis, Owen Goodchild, Róisín Ferguson, Zhihan Zhao
