"""Prompt construction for job skill extraction."""


def _build_job_skill_extraction_prompt(
    *,
    known_list: list[str],
    user_skills: list[str],
    cleaned: str,
) -> str:
    known_skills_prompt = ", ".join(known_list[:300])
    user_skills_prompt = ", ".join(user_skills)
    return (
        "Task: Extract required professional/technical skills from the job text.\n"
        "Known skills (prefer these): "
        f"{known_skills_prompt}\n\n"
        "Candidate skills from user profile (extra context): "
        f"{user_skills_prompt}\n\n"
        "Hard rules:\n"
        "1) Return only concrete skill entities (tools, languages, frameworks, methods, domains, certifications).\n"
        "2) Exclude all narrative, hiring, company, and generic phrases.\n"
        "3) Exclude pronoun-led, hiring, marketing, editorial, and business narrative phrasing.\n"
        "4) Do NOT return sentence fragments or clauses.\n"
        "5) Every returned skill must be supported by exact evidence from the text.\n"
        "6) First select explicitly required skills from Known skills.\n"
        "7) Add new skills only if strongly relevant and explicitly required.\n"
        "8) Skill names only (up to 4 words), translated to english and lowercase.\n"
        "9) Remove qualitative adjectives from skill names (example: 'good software design' -> 'software design').\n\n"
        "10) Remove education prefixes from skill names (example: 'degree in electrical engineering' -> 'electrical engineering').\n\n"
        "11) Remove qualification prefixes from skill names (example: 'experience with microsoft dynamics 365' -> 'microsoft dynamics 365').\n\n"
        "Validation before output:\n"
        "- If name has stopwords-only business phrasing, drop it.\n"
        "- If no concrete skills found, return empty arrays.\n\n"
        "Output format (strict JSON) with keys matched_known and new_candidates, each an array of objects: "
        "{\"name\": string, \"confidence\": number, \"evidence\": string}.\n\n"
        f"Description:\n{cleaned}\n\n"
        "JSON:"
    )
