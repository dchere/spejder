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
