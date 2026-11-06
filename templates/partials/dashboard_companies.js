            function getSelectedCompanyRows() {
                const table = document.getElementById('companies-table');
                if (!table) return [];
                return Array.from(table.querySelectorAll('tbody tr')).filter((row) => {
                    const checkbox = row.querySelector('.company-row-select');
                    return checkbox && checkbox.checked;
                });
            }

            function companiesLocalTodayKey() {
                const now = new Date();
                const y = now.getFullYear();
                const m = String(now.getMonth() + 1).padStart(2, '0');
                const d = String(now.getDate()).padStart(2, '0');
                return `${y}-${m}-${d}`;
            }

            function syncCompanyRowHighlights() {
                const table = document.getElementById('companies-table');
                if (!table) return;
                const todayKey = companiesLocalTodayKey();
                table.querySelectorAll('tbody tr').forEach((row) => {
                    const checkbox = row.querySelector('.company-row-select');
                    row.classList.toggle(
                        'company-row-selected',
                        Boolean(checkbox && checkbox.checked)
                    );
                    const addedKey = String(row.dataset.sortAddedAt || '').slice(0, 10);
                    row.classList.toggle(
                        'company-row-added-today',
                        Boolean(addedKey && addedKey !== '0000' && addedKey === todayKey)
                    );
                });
            }

            function updateCompaniesBulkBar() {
                const rows = getSelectedCompanyRows();
                const label = document.getElementById('companies-selected-label');
                const mergeBtn = document.getElementById('btn-merge-companies');
                const deleteBtn = document.getElementById('btn-delete-company');
                const selectAll = document.getElementById('companies-select-all');
                const table = document.getElementById('companies-table');
                const rowCheckboxes = table ? Array.from(table.querySelectorAll('.company-row-select')) : [];
                const count = rows.length;
                syncCompanyRowHighlights();
                if (label) {
                    label.textContent = count === 1 ? '1 selected' : `${count} selected`;
                }
                if (mergeBtn) mergeBtn.disabled = count < 2;
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

            function toggleSelectAllCompanies(checked) {
                const table = document.getElementById('companies-table');
                if (!table) return;
                table.querySelectorAll('.company-row-select').forEach((checkbox) => {
                    checkbox.checked = checked;
                });
                updateCompaniesBulkBar();
            }

            function bulkCompanyDeleteConfirmMessage(rows) {
                const names = rows.map((row) => row.dataset.displayName || '');
                const preview = names.length <= 5
                    ? names.join(', ')
                    : `${names.slice(0, 5).join(', ')} and ${names.length - 5} more`;
                const jobsTotal = rows.reduce(
                    (sum, row) => sum + (Number(row.dataset.jobCount) || 0),
                    0
                );
                return (
                    `Delete ${rows.length} compan${rows.length === 1 ? 'y' : 'ies'} (${preview}) ` +
                    `and ${jobsTotal} job(s)? This cannot be undone.`
                );
            }

            async function mergeSelectedCompanies(btnEl) {
                const rows = getSelectedCompanyRows();
                if (rows.length < 2) return;
                const names = rows.map((row) => row.dataset.displayName || '');
                const defaultName = names[0] || '';
                const displayName = prompt(
                    `Merge ${rows.length} companies into one display name:`,
                    defaultName
                );
                if (displayName === null) return;
                const trimmed = displayName.trim();
                if (!trimmed) {
                    alert('Display name is required');
                    return;
                }
                if (!confirm(`Merge ${rows.length} companies as "${trimmed}"? Position duplicates will be merged automatically.`)) {
                    return;
                }
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/merge'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_ids: rows.map((row) => Number(row.dataset.companyId)),
                            display_name: trimmed,
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (typeof waitForRebuildThenReload === 'function') {
                        await waitForRebuildThenReload('companies');
                    } else {
                        window.location.reload();
                    }
                } catch (err) {
                    alert(`Failed to merge companies: ${err.message}`);
                    updateCompaniesBulkBar();
                }
            }

            async function renameCompanyRow(companyId, displayName, btnEl) {
                const current = displayName || '';
                const nextName = prompt('Rename company to:', current);
                if (nextName === null) return;
                const trimmed = nextName.trim();
                if (!trimmed) {
                    alert('Display name is required');
                    return;
                }
                if (trimmed === current.trim()) return;
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/rename'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_id: Number(companyId),
                            display_name: trimmed,
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (typeof waitForRebuildThenReload === 'function') {
                        await waitForRebuildThenReload('companies');
                    } else {
                        window.location.reload();
                    }
                } catch (err) {
                    alert(`Failed to rename company: ${err.message}`);
                    if (btnEl) btnEl.disabled = false;
                }
            }

            async function reloadCompaniesAfterDelete(rebuilt) {
                if (rebuilt && typeof waitForRebuildThenReload === 'function') {
                    await waitForRebuildThenReload('companies');
                } else if (typeof reloadWithTab === 'function') {
                    reloadWithTab('companies');
                } else {
                    window.location.reload();
                }
            }

            async function deleteSelectedCompanies(btnEl) {
                const rows = getSelectedCompanyRows();
                if (rows.length === 0) return;
                if (!confirm(bulkCompanyDeleteConfirmMessage(rows))) return;
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/delete-batch'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_ids: rows.map((row) => Number(row.dataset.companyId)),
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    await reloadCompaniesAfterDelete((data.deleted_ids || []).length > 0);
                } catch (err) {
                    alert(`Failed to delete companies: ${err.message}`);
                    updateCompaniesBulkBar();
                }
            }

            async function deleteCompanyRow(companyId, displayName, jobCount, btnEl) {
                const name = displayName || '';
                const jobs = Number(jobCount) || 0;
                if (!confirm(
                    `Delete company "${name}" and its ${jobs} job(s)? This cannot be undone.`
                )) {
                    return;
                }
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/delete'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_id: Number(companyId),
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    await reloadCompaniesAfterDelete(!data.already_gone);
                } catch (err) {
                    alert(`Failed to delete company: ${err.message}`);
                    if (btnEl) btnEl.disabled = false;
                }
            }
