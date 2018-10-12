select *
from study_category
into outfile '/data/mysql/neurobank_study_category.tsv';

select * from study_subject
join subject_property on subject_property.subject_id = study_subject.subject_id
join study_category on study_category.category = subject_property.property_category
into outfile '/data/mysql/neurobank_subject_property2.tsv';

select * from subject_timepoint
join timepoint_event on timepoint_event.timepoint_id = subject_timepoint.timepoint_id
join study_subject on study_subject.subject_id = subject_timepoint.subject_id
into outfile '/data/mysql/neurobank_subject_timepoint.tsv';

select * from timepoint_event_property
join timepoint_event on timepoint_event.timepoint_event_id =
  timepoint_event_property.timepoint_event_id
join study_category on study_category.category =
 timepoint_event_property.property_category
 join study_subject on
into outfile '/data/mysql/neurobank_timepoint_event_property';

-- single result file for subject timepoint properties
select study_subject.subject_id, study_subject.subject_guid,
 subject_timepoint.timepoint_name, subject_timepoint.timepoint_id,
 timepoint_event.event_category, timepoint_event.form_name,
timepoint_event_property.property_category, timepoint_event_property.property_code,
timepoint_event_property.property_name, timepoint_event_property.property_value
from study_subject
join subject_timepoint on subject_timepoint.subject_id = study_subject.subject_id
join timepoint_event on timepoint_event.timepoint_id = subject_timepoint.timepoint_id
join timepoint_event_property on timepoint_event_property.timepoint_event_id = timepoint_event.timepoint_event_id
into outfile '/data/mysql/neurobank_event_property2.tsv';




