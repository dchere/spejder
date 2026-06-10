import re

from spejder.db.utils import _is_djinni_position_link


def _is_job_link(link: str) -> bool:
    low = link.lower()
    if "linkedin.com/comm/jobs/view/" in low or "linkedin.com/jobs/view/" in low:
        return True
    if "thehub.io/jobs/" in low and re.search(r"thehub\.io/jobs/[0-9a-f]{12,}", low):
        return True
    if re.search(
        r"(?:careers\.google\.com|google\.com)/.+/jobs/results/\d+",
        low,
    ):
        return True
    if "jobindex.dk" in low and (
        "jobid=" in low
        or re.search(r"/jobannonce/[hr]\d+", low)
        or re.search(r"/bruger/dine-job/[hr]\d+", low)
    ):
        return True
    if "careers.demant.com" in low and "/job/" in low:
        return True
    if "jobs.danfoss.com" in low and "/job/" in low:
        return True
    if "jobs.teradyne.com" in low and "/job/" in low:
        return True
    if "careers.nordea.com" in low and "/job/" in low:
        return True
    if "careers.novonordisk.com" in low and "/job/" in low:
        return True
    if "careers.vestas.com" in low and re.search(r"/job/.+/\d+", low):
        return True
    if (
        re.search(r"\.fa\.[a-z0-9]+\.oraclecloud\.com", low)
        and "/candidateexperience/" in low
        and re.search(r"/job/\d+", low)
    ):
        return True
    if (
        re.search(r"\.fa\.ocs\.oraclecloud\.(?:com|eu)", low)
        and "/candidateexperience/" in low
        and re.search(r"/job/\d+", low)
    ):
        return True
    if "careers.nttdata-solutions.com" in low and "/job/" in low:
        return True
    if "careers.getinge.com" in low and "/job/" in low:
        return True
    if "jobs.tetrapak.com" in low and re.search(r"/job/[^/]+/\d+", low):
        return True
    if _is_djinni_position_link(link):
        return True
    return False


