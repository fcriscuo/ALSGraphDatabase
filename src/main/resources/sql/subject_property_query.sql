select * from string_subject_property
join subject on subject.subject_id = string_subject_property.subject_id
join sample on sample.subject_id = subject.subject_id
where subject.external_subject_id like 'NEU%';



-- sql commands to export query output to file
-- set search_path to metronome_b38;
-- \a
-- \t
-- \pset fieldsep '\t'
--

-- select ......
-- \o
-- \! ls -l /tmp/string_subject_property.tsv

select * from sample_group_membership
join sample_group on sample_group.sample_group_id = sample_group_membership.sample_group_id
where sample_group.nygc_sample_group_id like 'CGND%';

select sample.external_sample_id, sample_gene_variant_summary.*
from sample_gene_variant_summary,sample
where sample_gene_variant_summary.sample_id in (
select sample_group_membership.sample_id from sample_group_membership
join sample_group on sample_group.sample_group_id = sample_group_membership.sample_group_id
where sample_group.nygc_sample_group_id like 'CGND%')
and sample.sample_id = sample_gene_variant_summary.sample_id;
