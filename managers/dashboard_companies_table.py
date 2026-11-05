"""Companies tab HTML for the main dashboard."""

import html as html_lib
import json


def _render_companies_table_html(companies_items: list[dict]) -> str:
    rows: list[str] = []
    for item in companies_items:
        company_id = int(item.get("id") or 0)
        display_name = str(item.get("display_name") or "")
        job_count = int(item.get("job_count") or 0)
        relevant_count = int(item.get("relevant_count") or 0)
        not_relevant_count = int(item.get("not_relevant_count") or 0)
        applied_count = int(item.get("applied_count") or 0)
        interview_count = int(item.get("interview_count") or 0)
        stopped_count = int(item.get("stopped_count") or 0)
        alias_count = int(item.get("alias_count") or 0)
        safe_name = html_lib.escape(display_name)
        name_js = html_lib.escape(json.dumps(display_name), quote=True)
        rows.append(
            f"""
            <tr data-company-id="{company_id}"
                data-display-name="{html_lib.escape(display_name, quote=True)}"
                data-job-count="{job_count}"
                data-sort-name="{html_lib.escape(display_name, quote=True)}"
                data-sort-job-count="{job_count}"
                data-sort-relevant="{relevant_count}"
                data-sort-not-relevant="{not_relevant_count}"
                data-sort-applied="{applied_count}"
                data-sort-interview="{interview_count}"
                data-sort-stopped="{stopped_count}"
                data-sort-alias-count="{alias_count}">
                <td><input type="checkbox" class="company-row-select" aria-label="Select company" onchange="updateCompaniesBulkBar()" /></td>
                <td><button type="button" class="pseudo-link company-name-link" onclick="openCompanyPage({name_js})">{safe_name}</button></td>
                <td>{job_count}</td>
                <td>{relevant_count}</td>
                <td>{not_relevant_count}</td>
                <td>{applied_count}</td>
                <td>{interview_count}</td>
                <td>{stopped_count}</td>
                <td>{alias_count}</td>
            </tr>
            """.strip()
        )

    if not rows:
        return '<p class="empty">No companies found.</p>'

    return (
        """
        <table class="companies-table" id="companies-table">
            <thead>
                <tr>
                    <th title="Select companies for Merge, Rename, or Delete."><input type="checkbox" id="companies-select-all" aria-label="Select all companies" onchange="toggleSelectAllCompanies(this.checked)" /></th>
                    <th class="companies-sortable" data-sort-key="name" title="Catalog display name. Click to open the company page. Click header to sort.">Company<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable companies-sort-active companies-sort-desc" data-sort-key="job_count" aria-sort="descending" title="Jobs whose company key matches any alias of this catalog row (all stages; soft-hidden included). Click to sort.">Jobs<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="relevant" title="Non-applied jobs with category Relevant (soft-hidden included). Click to sort.">Relevant<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="not_relevant" title="Non-applied jobs with category Not relevant (soft-hidden included). Click to sort.">Not relevant<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="applied" title="Applied jobs not on interview or stopped (soft-hidden included). Click to sort.">Applied<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="interview" title="Applied jobs on interview (soft-hidden included). Click to sort.">Interview<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="stopped" title="Applied jobs with interview stopped (soft-hidden included). Click to sort.">Stopped<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="alias_count" title="Number of alias strings (including display name). Click to sort.">Aliases<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                </tr>
            </thead>
            <tbody>
        """
        + "".join(rows)
        + """
            </tbody>
        </table>
        """
    )
