            function getSelectedCompanyRows() {
                const table = document.getElementById('companies-table');
                if (!table) return [];
                return Array.from(table.querySelectorAll('tbody tr')).filter((row) => {
                    const checkbox = row.querySelector('.company-row-select');
                    return checkbox && checkbox.checked;
                });
            }

            function updateCompaniesBulkBar() {
                const rows = getSelectedCompanyRows();
                const label = document.getElementById('companies-selected-label');
                const mergeBtn = document.getElementById('btn-merge-companies');
                const renameBtn = document.getElementById('btn-rename-company');
                const deleteBtn = document.getElementById('btn-delete-company');
                const selectAll = document.getElementById('companies-select-all');
                const table = document.getElementById('companies-table');
                const rowCheckboxes = table ? Array.from(table.querySelectorAll('.company-row-select')) : [];
                const count = rows.length;
                if (label) {
                    label.textContent = count === 1 ? '1 selected' : `${count} selected`;
                }
                if (mergeBtn) mergeBtn.disabled = count < 2;
                if (renameBtn) renameBtn.disabled = count !== 1;
                if (deleteBtn) deleteBtn.disabled = count !== 1;
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

            async function renameSelectedCompany(btnEl) {
                const rows = getSelectedCompanyRows();
                if (rows.length !== 1) return;
                const row = rows[0];
                const current = row.dataset.displayName || '';
                const displayName = prompt('Rename company to:', current);
                if (displayName === null) return;
                const trimmed = displayName.trim();
                if (!trimmed) {
                    alert('Display name is required');
                    return;
                }
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/rename'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_id: Number(row.dataset.companyId),
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
                    updateCompaniesBulkBar();
                }
            }

            async function deleteSelectedCompany(btnEl) {
                const rows = getSelectedCompanyRows();
                if (rows.length !== 1) return;
                const row = rows[0];
                const name = row.dataset.displayName || '';
                const jobCount = Number(row.dataset.jobCount || 0);
                if (!confirm(
                    `Delete company "${name}" and its ${jobCount} job(s)? This cannot be undone.`
                )) {
                    return;
                }
                if (btnEl) btnEl.disabled = true;
                try {
                    const response = await fetch(apiUrl('/api/company/delete'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_id: Number(row.dataset.companyId),
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
                    alert(`Failed to delete company: ${err.message}`);
                    updateCompaniesBulkBar();
                }
            }
