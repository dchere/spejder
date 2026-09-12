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
