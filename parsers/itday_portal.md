# spejder.parsers.itday_portal

**Purpose:**
Fetch and parse listings from the IT-DAY Wix job portal (`https://www.itday.dk/job-portal`).

**API:**
- `ITDAY_PORTAL_URL` — portal base URL
- `ITDAY_PORTAL_SOURCE` — stored `source` label (`IT-DAY Job Portal`)
- `fetch_itday_portal_html(page=1, *, timeout_sec=15) -> str`
- `parse_itday_portal_html(html_text) -> list[dict]` — job entry dicts with `position_link`, `company`, `title`, `place`, `work_type`, `raw_text`, `source`
- `fetch_itday_portal_entries(*, max_pages=10, timeout_sec=15) -> list[dict]`

**Pagination:**
Wix renders page controls client-side without changing the browser URL. Server-side pagination uses `?dynamic_page=N` (`N=1` is the default listing). Fetch stops on the first empty page or when a page adds no new links.

**Parsing:**
Each card is a `.wixui-repeater__item`. Company/title come from `.wixui-collapsible-text__text`; place and opportunity type from the first two `h2` elements. Cards link to external ATS/apply URLs; internal IT-DAY pages are included only for `/praktik`. Intern/`praktik` listings stay in scope as jobs. Social/footer links are skipped, including LinkedIn `/in/`, `/company/`, and share URLs; `linkedin.com/jobs/` apply URLs are kept. Listing `http://` links are canonicalized to `https://` after `urljoin`. Duplicate card hrefs are dropped.

**Context:**
Used by `spejder.workflows.portal_sync.sync_itday_portal`, which upserts entries through `ingest_entries_to_db`. No JavaScript runtime is required.
