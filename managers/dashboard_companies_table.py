"""Companies tab HTML for the main dashboard."""

import html as html_lib
import json

_COMPANY_ICON_SVG_ATTRS = (
    'xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" '
    'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round" aria-hidden="true"'
)
_RENAME_COMPANY_ICON_SVG = (
    f"<svg {_COMPANY_ICON_SVG_ATTRS}>"
    '<path d="M12 20h9"/>'
    '<path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z"/>'
    "</svg>"
)
_DELETE_COMPANY_ICON_SVG = (
    f"<svg {_COMPANY_ICON_SVG_ATTRS}>"
    '<path d="M3 6h18"/>'
    '<path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/>'
    '<path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>'
    "</svg>"
)
_EMPTY_ADDED_AT_SORT = "0000"


def _render_companies_table_html(companies_items: list[dict]) -> str:
    rows: list[str] = []
    for item in companies_items:
        company_id = int(item.get("id") or 0)
        display_name = str(item.get("display_name") or "")
        created_at_raw = str(item.get("created_at") or "")
        job_count = int(item.get("job_count") or 0)
        not_viewed_count = int(item.get("not_viewed_count") or 0)
        relevant_count = int(item.get("relevant_count") or 0)
        not_relevant_count = int(item.get("not_relevant_count") or 0)
        applied_count = int(item.get("applied_count") or 0)
        interview_count = int(item.get("interview_count") or 0)
        stopped_count = int(item.get("stopped_count") or 0)
        alias_count = int(item.get("alias_count") or 0)
        safe_name = html_lib.escape(display_name)
        name_js = html_lib.escape(json.dumps(display_name), quote=True)
        if created_at_raw:
            added_display = html_lib.escape(created_at_raw[:10])
            added_title = html_lib.escape(f"Catalog row created: {created_at_raw}")
            sort_added_at = html_lib.escape(created_at_raw, quote=True)
        else:
            added_display = "—"
            added_title = html_lib.escape("Catalog row has no created_at")
            sort_added_at = _EMPTY_ADDED_AT_SORT
        rows.append(
            f"""
            <tr data-company-id="{company_id}"
                data-display-name="{html_lib.escape(display_name, quote=True)}"
                data-job-count="{job_count}"
                data-sort-name="{html_lib.escape(display_name, quote=True)}"
                data-sort-added-at="{sort_added_at}"
                data-sort-job-count="{job_count}"
                data-sort-not-viewed="{not_viewed_count}"
                data-sort-relevant="{relevant_count}"
                data-sort-not-relevant="{not_relevant_count}"
                data-sort-applied="{applied_count}"
                data-sort-interview="{interview_count}"
                data-sort-stopped="{stopped_count}"
                data-sort-alias-count="{alias_count}">
                <td><input type="checkbox" class="company-row-select" aria-label="Select company" onchange="updateCompaniesBulkBar()" /></td>
                <td class="companies-action"><button type="button" class="rename-company-btn" onclick="renameCompanyRow({company_id}, {name_js}, this)" title="Rename company" aria-label="Rename company">{_RENAME_COMPANY_ICON_SVG}</button><button type="button" class="delete-company-btn" onclick="deleteCompanyRow({company_id}, {name_js}, {job_count}, this)" title="Delete company" aria-label="Delete company">{_DELETE_COMPANY_ICON_SVG}</button></td>
                <td><button type="button" class="pseudo-link company-name-link" onclick="openCompanyPage({name_js})">{safe_name}</button></td>
                <td title="{added_title}">{added_display}</td>
                <td>{job_count}</td>
                <td>{not_viewed_count}</td>
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
                    <th title="Select companies for Merge or Delete."><input type="checkbox" id="companies-select-all" aria-label="Select all companies" onchange="toggleSelectAllCompanies(this.checked)" /></th>
                    <th class="companies-action" title="Rename company display name. Delete company and all membership jobs.">Action</th>
                    <th class="companies-sortable" data-sort-key="name" title="Catalog display name. Click to open the company page. Click header to sort.">Company<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable companies-sort-active companies-sort-desc" data-sort-key="added_at" aria-sort="descending" title="When the catalog row was created (companies.created_at). Click to sort.">Added<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="job_count" title="Jobs whose company key matches any alias of this catalog row (all stages; soft-hidden included). Click to sort.">Jobs<span class="companies-sort-indicator" aria-hidden="true"></span></th>
                    <th class="companies-sortable" data-sort-key="not_viewed" title="Membership jobs with viewed=0 (all stages; soft-hidden included). Click to sort.">Not viewed<span class="companies-sort-indicator" aria-hidden="true"></span></th>
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
