            function initCompaniesTableSort() {
                const table = document.getElementById('companies-table');
                if (!table) return;

                const tbody = table.querySelector('tbody');
                const headers = Array.from(table.querySelectorAll('th.companies-sortable'));
                if (!tbody || headers.length === 0) return;

                const sortState = { key: 'added_at', dir: -1 };

                function rowValue(row, key) {
                    if (key === 'name') return (row.dataset.sortName || '').toLowerCase();
                    if (key === 'added_at') return row.dataset.sortAddedAt || '';
                    if (key === 'job_count') return Number.parseInt(row.dataset.sortJobCount || '0', 10) || 0;
                    if (key === 'not_viewed') return Number.parseInt(row.dataset.sortNotViewed || '0', 10) || 0;
                    if (key === 'relevant') return Number.parseInt(row.dataset.sortRelevant || '0', 10) || 0;
                    if (key === 'not_relevant') return Number.parseInt(row.dataset.sortNotRelevant || '0', 10) || 0;
                    if (key === 'applied') return Number.parseInt(row.dataset.sortApplied || '0', 10) || 0;
                    if (key === 'interview') return Number.parseInt(row.dataset.sortInterview || '0', 10) || 0;
                    if (key === 'stopped') return Number.parseInt(row.dataset.sortStopped || '0', 10) || 0;
                    if (key === 'alias_count') return Number.parseInt(row.dataset.sortAliasCount || '0', 10) || 0;
                    return '';
                }

                function compareRows(a, b) {
                    const left = rowValue(a, sortState.key);
                    const right = rowValue(b, sortState.key);
                    let cmp = 0;
                    if (typeof left === 'number' && typeof right === 'number') {
                        cmp = left - right;
                    } else {
                        cmp = String(left).localeCompare(String(right));
                    }
                    if (cmp !== 0) {
                        return cmp * sortState.dir;
                    }
                    return String(a.dataset.sortName || '').localeCompare(String(b.dataset.sortName || ''));
                }

                function applySort() {
                    const rows = Array.from(tbody.querySelectorAll('tr'));
                    rows.sort(compareRows);
                    rows.forEach((row) => tbody.appendChild(row));
                    headers.forEach((header) => {
                        const active = header.dataset.sortKey === sortState.key;
                        header.classList.toggle('companies-sort-active', active);
                        header.classList.toggle('companies-sort-desc', active && sortState.dir < 0);
                        if (active) {
                            header.setAttribute('aria-sort', sortState.dir > 0 ? 'ascending' : 'descending');
                        } else {
                            header.removeAttribute('aria-sort');
                        }
                    });
                }

                headers.forEach((header) => {
                    header.setAttribute('tabindex', '0');
                    header.setAttribute('role', 'button');
                    const activateSort = () => {
                        const key = header.dataset.sortKey || 'name';
                        if (sortState.key === key) {
                            sortState.dir *= -1;
                        } else {
                            sortState.key = key;
                            sortState.dir = (key === 'name') ? 1 : -1;
                        }
                        applySort();
                    };
                    header.addEventListener('click', activateSort);
                    header.addEventListener('keydown', (event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            activateSort();
                        }
                    });
                });

                applySort();
            }
