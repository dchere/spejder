            let skillsTableSort = null;

            function syncSkillsRowSortDataset(row, sortKey, enabled) {
                if (sortKey === 'has_skill') {
                    row.dataset.sortHasSkill = enabled ? '1' : '0';
                } else if (sortKey === 'want_learn') {
                    row.dataset.sortWantLearn = enabled ? '1' : '0';
                } else if (sortKey === 'not_for_me') {
                    row.dataset.sortNotForMe = enabled ? '1' : '0';
                }
                if (skillsTableSort && skillsTableSort.sortKey() === sortKey) {
                    skillsTableSort.applySort();
                }
            }

            function skillFlagBoxes(row) {
                if (!row) return null;
                return {
                    has: row.querySelector('.skill-has-checkbox'),
                    learn: row.querySelector('.skill-learn-checkbox'),
                    unwanted: row.querySelector('.skill-unwanted-checkbox'),
                };
            }

            function readSkillRowFlags(row) {
                const boxes = skillFlagBoxes(row);
                if (!boxes) return null;
                return {
                    has: !!(boxes.has && boxes.has.checked),
                    learn: !!(boxes.learn && boxes.learn.checked),
                    unwanted: !!(boxes.unwanted && boxes.unwanted.checked),
                };
            }

            function restoreSkillRowFlags(row, flags) {
                if (!row || !flags) return;
                const boxes = skillFlagBoxes(row);
                if (!boxes) return;
                if (boxes.has) boxes.has.checked = flags.has;
                if (boxes.learn) boxes.learn.checked = flags.learn;
                if (boxes.unwanted) boxes.unwanted.checked = flags.unwanted;
                syncSkillsRowSortDataset(row, 'has_skill', flags.has);
                syncSkillsRowSortDataset(row, 'want_learn', flags.learn);
                syncSkillsRowSortDataset(row, 'not_for_me', flags.unwanted);
            }

            function uncheckSkillFlag(row, checkboxClass, sortKey) {
                if (!row) return;
                const box = row.querySelector('.' + checkboxClass);
                if (!box || !box.checked) return;
                box.checked = false;
                syncSkillsRowSortDataset(row, sortKey, false);
            }

            function focusSkillRow(skillKey) {
                if (!skillKey) return;
                const row = panelSkills.querySelector(`tr[data-skill-key="${CSS.escape(skillKey)}"]`);
                if (!row) return;
                panelSkills.querySelectorAll('tbody tr.skill-row-focus').forEach((item) => item.classList.remove('skill-row-focus'));
                row.classList.add('skill-row-focus');
                row.setAttribute('tabindex', '-1');
                row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                window.setTimeout(() => row.focus({ preventScroll: true }), 120);
                window.setTimeout(() => row.classList.remove('skill-row-focus'), 2200);
            }

            function openSkillsForSkill(skillKey) {
                setMode('skills');
                focusSkillRow(skillKey);
            }

            async function setUserSkill(skillKey, hasSkill, inputEl) {
                const row = inputEl.closest('tr');
                const prev = readSkillRowFlags(row);
                if (hasSkill) {
                    uncheckSkillFlag(row, 'skill-unwanted-checkbox', 'not_for_me');
                }
                inputEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/user'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skill: skillKey, has_skill: hasSkill })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (row) {
                        syncSkillsRowSortDataset(row, 'has_skill', hasSkill);
                    }
                } catch (err) {
                    restoreSkillRowFlags(row, prev);
                    alert(`Failed to update skill: ${err.message}`);
                } finally {
                    inputEl.disabled = false;
                }
            }

            async function setLearnSkill(skillKey, learn, inputEl) {
                const row = inputEl.closest('tr');
                const prev = readSkillRowFlags(row);
                if (learn) {
                    uncheckSkillFlag(row, 'skill-unwanted-checkbox', 'not_for_me');
                }
                inputEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/learn'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skill: skillKey, learn })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (row) {
                        syncSkillsRowSortDataset(row, 'want_learn', learn);
                    }
                } catch (err) {
                    restoreSkillRowFlags(row, prev);
                    alert(`Failed to update skill: ${err.message}`);
                } finally {
                    inputEl.disabled = false;
                }
            }

            async function setUnwantedSkill(skillKey, unwanted, inputEl) {
                const row = inputEl.closest('tr');
                const prev = readSkillRowFlags(row);
                if (unwanted) {
                    uncheckSkillFlag(row, 'skill-has-checkbox', 'has_skill');
                    uncheckSkillFlag(row, 'skill-learn-checkbox', 'want_learn');
                }
                inputEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/unwanted'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skill: skillKey, unwanted })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (row) {
                        syncSkillsRowSortDataset(row, 'not_for_me', unwanted);
                    }
                } catch (err) {
                    restoreSkillRowFlags(row, prev);
                    alert(`Failed to update skill: ${err.message}`);
                } finally {
                    inputEl.disabled = false;
                }
            }

            function getSelectedSkillKeys() {
                const table = document.getElementById('skills-table');
                if (!table) return [];
                return Array.from(table.querySelectorAll('tbody tr'))
                    .filter((row) => {
                        const checkbox = row.querySelector('.skill-row-select');
                        return checkbox && checkbox.checked;
                    })
                    .map((row) => row.dataset.skillKey || '')
                    .filter(Boolean);
            }

            function updateSkillsBulkBar() {
                const keys = getSelectedSkillKeys();
                const label = document.getElementById('skills-selected-label');
                const blockBtn = document.getElementById('btn-block-selected');
                const deleteBtn = document.getElementById('btn-delete-selected');
                const selectAll = document.getElementById('skills-select-all');
                const table = document.getElementById('skills-table');
                const rowCheckboxes = table ? Array.from(table.querySelectorAll('.skill-row-select')) : [];
                const count = keys.length;
                if (label) {
                    label.textContent = count === 1 ? '1 selected' : `${count} selected`;
                }
                if (blockBtn) blockBtn.disabled = count === 0;
                if (deleteBtn) deleteBtn.disabled = count === 0;
                if (selectAll && rowCheckboxes.length > 0) {
                    const checkedCount = rowCheckboxes.filter((checkbox) => checkbox.checked).length;
                    selectAll.checked = checkedCount === rowCheckboxes.length;
                    selectAll.indeterminate = checkedCount > 0 && checkedCount < rowCheckboxes.length;
                } else if (selectAll) {
                    selectAll.checked = false;
                    selectAll.indeterminate = false;
                }
            }

            function toggleSelectAllSkills(checked) {
                const table = document.getElementById('skills-table');
                if (!table) return;
                table.querySelectorAll('.skill-row-select').forEach((checkbox) => {
                    checkbox.checked = checked;
                });
                updateSkillsBulkBar();
            }

            function bulkSkillConfirmMessage(action, keys) {
                const preview = keys.length <= 5
                    ? keys.join(', ')
                    : `${keys.slice(0, 5).join(', ')} and ${keys.length - 5} more`;
                if (action === 'block') {
                    return `Block ${keys.length} skill(s) (${preview}) and hide them from all positions and the skills tab?`;
                }
                return `Delete ${keys.length} skill(s) (${preview}) from profile and DB?`;
            }

            function removeSkillRows(skillKeys) {
                const removed = new Set(skillKeys);
                const table = document.getElementById('skills-table');
                if (!table) return;
                Array.from(table.querySelectorAll('tbody tr')).forEach((row) => {
                    if (removed.has(row.dataset.skillKey)) {
                        row.remove();
                    }
                });
            }

            async function blockSelectedSkills(btnEl) {
                const keys = getSelectedSkillKeys();
                if (keys.length === 0) return;
                if (!confirm(bulkSkillConfirmMessage('block', keys))) return;
                if (btnEl) btnEl.disabled = true;
                const deleteBtn = document.getElementById('btn-delete-selected');
                if (deleteBtn) deleteBtn.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/block-batch'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skills: keys }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    removeSkillRows(data.skills || keys);
                    refreshCounts();
                } catch (err) {
                    alert(`Failed to block skills: ${err.message}`);
                } finally {
                    updateSkillsBulkBar();
                }
            }

            async function deleteSelectedSkills(btnEl) {
                const keys = getSelectedSkillKeys();
                if (keys.length === 0) return;
                if (!confirm(bulkSkillConfirmMessage('delete', keys))) return;
                if (btnEl) btnEl.disabled = true;
                const blockBtn = document.getElementById('btn-block-selected');
                if (blockBtn) blockBtn.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/delete-batch'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skills: keys }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    removeSkillRows(data.skills || keys);
                    refreshCounts();
                } catch (err) {
                    alert(`Failed to delete skills: ${err.message}`);
                } finally {
                    updateSkillsBulkBar();
                }
            }

            async function deleteSkill(skillKey, btnEl) {
                if (!confirm(`Delete skill '${skillKey}' from profile and DB?`)) return;
                btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/delete'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skill: skillKey })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    const row = btnEl.closest('tr');
                    if (row) row.remove();
                    refreshCounts();
                    updateSkillsBulkBar();
                } catch (err) {
                    alert(`Failed to delete skill: ${err.message}`);
                    btnEl.disabled = false;
                }
            }

            async function blockSkill(skillKey, btnEl) {
                if (!confirm(`Block skill '${skillKey}' and hide it from all positions and the skills tab?`)) return;
                btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/skill/block'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ skill: skillKey })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    const row = btnEl.closest('tr');
                    if (row) row.remove();
                    refreshCounts();
                    updateSkillsBulkBar();
                } catch (err) {
                    alert(`Failed to block skill: ${err.message}`);
                    btnEl.disabled = false;
                }
            }

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
