"""Evidence labels, not eligibility decisions. No profession-specific exclusions."""
from datetime import datetime, timezone
import hashlib
import re
from sources import remote_confidence


def parse_date(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except (ValueError, TypeError):
        return None


def evidence(job, now=None):
    now = now or datetime.now(timezone.utc)
    posted = parse_date(job.get('date_posted'))
    expiry = parse_date(job.get('valid_through') or job.get('validThrough'))
    remote = remote_confidence(job)
    declared = job.get('is_remote')
    if declared is True:
        remote = 'source_declared_remote' if remote not in ('negative_or_mixed','hybrid','mixed') else 'conflicting_evidence'
    signature = '|'.join(re.sub(r'\W+', ' ',str(job.get(k) or '').casefold()).strip() for k in ('title','company','location'))
    return {'remote_evidence':remote,
            'possible_duplicate_group':hashlib.sha256(signature.encode()).hexdigest()[:16] if job.get('company') and job.get('title') else None,
            'source_remote_flag':declared if isinstance(declared,bool) else None,
            'posted_age_days':max(0,(now-posted).days) if posted else None,
            'expiry_evidence':'expired' if expiry and expiry < now else 'not_expired' if expiry else 'unknown',
            'availability':'not_reverified'}
