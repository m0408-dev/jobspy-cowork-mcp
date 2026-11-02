"""Conservative content-type labels, never applicant/profession filters.

Raw records are retained. Only high-confidence non-vacancies leave the default
candidate view; unknown records remain available for human/host inspection.
"""
import re

VERSION = 1


def classify(job):
    text = str(job.get('title') or '') + '\n' + str(job.get('description') or '')
    # Do not classify a company's ordinary recruitment paragraph as an ad.
    head = text[:650]
    if (re.search(r'^\s*Location\s*:', head, re.I)
            and re.search(r'\bWilling to relocate\s*:', text, re.I)):
        return 'job_seeker', 'candidate_profile_fields'
    if re.search(r'^\s*(?:seeking work|looking for (?:a |my next )?job|suche (?:eine )?stelle)\b', head, re.I):
        return 'job_seeker', 'explicit_work_wanted'
    if re.search(r'^.{0,100}\b(?:is a job aggregator|is a job board|job board with \d|job aggregator with)\b', head, re.I):
        return 'directory_promotion', 'explicit_directory_ad'
    if job.get('evidence_origin') in ('public_jobposting_jsonld', 'board_listing_card'):
        return 'vacancy_candidate', 'structured_listing'
    return 'unclassified', 'requires_content_review'
