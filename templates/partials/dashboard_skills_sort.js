            function initSkillsTableSort() {
                const table = document.getElementById('skills-table');
                if (!table) return;

                const tbody = table.querySelector('tbody');
                const headers = Array.from(table.querySelectorAll('th.skills-sortable'));
                if (!tbody || headers.length === 0) return;

                const SKILLS_EMPTY_ADDED_AT_SORT = {{ skills_empty_added_at_sort | tojson }};
                const sortState = { key: 'added_at', dir: -1 };

                function isEmptyAddedAt(value) {
                    return !value || value === SKILLS_EMPTY_ADDED_AT_SORT;
                }

                function rowValue(row, key) {
                    if (key === 'name') return (row.dataset.sortName || '').toLowerCase();
                    if (key === 'added_at') return row.dataset.sortAddedAt || '';
                    if (key === 'source') return (row.dataset.sortSource || '').toLowerCase();
                    if (key === 'position_pct') return Number.parseFloat(row.dataset.sortPositionPct || '0') || 0;
                    if (key === 'occurrences') return Number.parseInt(row.dataset.sortOccurrences || '0', 10) || 0;
                    if (key === 'has_skill') return Number.parseInt(row.dataset.sortHasSkill || '0', 10) || 0;
                    if (key === 'want_learn') return Number.parseInt(row.dataset.sortWantLearn || '0', 10) || 0;
                    if (key === 'not_for_me') return Number.parseInt(row.dataset.sortNotForMe || '0', 10) || 0;
                    return '';
                }

                function compareRows(a, b) {
                    const left = rowValue(a, sortState.key);
                    const right = rowValue(b, sortState.key);
                    let cmp = 0;
                    if (sortState.key === 'added_at') {
                        const leftEmpty = isEmptyAddedAt(left);
                        const rightEmpty = isEmptyAddedAt(right);
                        if (leftEmpty && !rightEmpty) {
                            return 1;
                        }
                        if (!leftEmpty && rightEmpty) {
                            return -1;
                        }
                        if (!leftEmpty && !rightEmpty) {
                            cmp = String(left).localeCompare(String(right));
                        }
                    } else if (typeof left === 'number' && typeof right === 'number') {
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
                        header.classList.toggle('skills-sort-active', active);
                        header.classList.toggle('skills-sort-desc', active && sortState.dir < 0);
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
                            sortState.dir = (key === 'name' || key === 'source') ? 1 : -1;
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

                skillsTableSort = {
                    applySort,
                    sortKey: () => sortState.key,
                };
                applySort();
            }
