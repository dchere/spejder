            let companyMergeState = null;

            function isCompanyMergeInFlight() {
                return !!(companyMergeState && companyMergeState.inFlight);
            }

            function setCompanyMergeDialogBusy(busy) {
                const confirmBtn = document.getElementById('company-merge-confirm');
                const cancel = document.getElementById('company-merge-cancel');
                if (confirmBtn) confirmBtn.disabled = busy;
                if (cancel) cancel.disabled = busy;
            }

            function buildCompanyMergeCandidates(rows) {
                const candidates = rows.map((row) => ({
                    id: Number(row.dataset.companyId),
                    name: row.dataset.displayName || '',
                    jobCount: Number(row.dataset.jobCount) || 0,
                }));
                // Stable sort: job count DESC; ties keep getSelectedCompanyRows() order.
                candidates.sort((a, b) => b.jobCount - a.jobCount);
                return candidates;
            }

            function onCompanyMergeKeydown(event) {
                if (event.key !== 'Escape') return;
                if (isCompanyMergeInFlight()) return;
                event.preventDefault();
                closeCompanyMergeModal();
            }

            function closeCompanyMergeModal(options) {
                const force = !!(options && options.force);
                if (isCompanyMergeInFlight() && !force) return;
                const modal = document.getElementById('company-merge-modal');
                if (modal) {
                    modal.classList.add('hidden');
                    modal.setAttribute('hidden', '');
                    modal.setAttribute('aria-hidden', 'true');
                }
                companyMergeState = null;
                setCompanyMergeDialogBusy(false);
                document.removeEventListener('keydown', onCompanyMergeKeydown);
                if (typeof updateCompaniesBulkBar === 'function') {
                    updateCompaniesBulkBar();
                }
            }

            function openCompanyMergeModal(candidates) {
                if (isCompanyMergeInFlight()) return;
                const modal = document.getElementById('company-merge-modal');
                const title = document.getElementById('company-merge-title');
                const input = document.getElementById('company-merge-display-name');
                const list = document.getElementById('company-merge-name-list');
                const confirmBtn = document.getElementById('company-merge-confirm');
                const cancel = document.getElementById('company-merge-cancel');
                const mergeBtn = document.getElementById('btn-merge-companies');
                if (!modal || !input || !list || candidates.length < 2) return;

                companyMergeState = {
                    ids: candidates.map((c) => c.id),
                    inFlight: false,
                };
                if (title) {
                    title.textContent = `Merge ${candidates.length} companies`;
                }
                input.value = candidates[0].name || '';
                if (confirmBtn) confirmBtn.disabled = false;
                if (cancel) cancel.disabled = false;
                if (mergeBtn) mergeBtn.disabled = true;

                list.replaceChildren();
                candidates.forEach((candidate) => {
                    const item = document.createElement('li');
                    item.className = 'company-merge-name-item';
                    item.textContent = candidate.name;
                    item.title = 'Click to use as display name';
                    item.addEventListener('click', () => {
                        input.value = candidate.name;
                        input.focus();
                        input.select();
                    });
                    list.appendChild(item);
                });

                modal.classList.remove('hidden');
                modal.removeAttribute('hidden');
                modal.setAttribute('aria-hidden', 'false');
                document.removeEventListener('keydown', onCompanyMergeKeydown);
                document.addEventListener('keydown', onCompanyMergeKeydown);
                input.focus();
                input.select();
            }

            function mergeSelectedCompanies(_btnEl) {
                const rows = getSelectedCompanyRows();
                if (rows.length < 2) return;
                openCompanyMergeModal(buildCompanyMergeCandidates(rows));
            }

            async function confirmCompanyMerge() {
                if (!companyMergeState || companyMergeState.inFlight) return;
                const input = document.getElementById('company-merge-display-name');
                const trimmed = (input ? input.value : '').trim();
                if (!trimmed) {
                    alert('Display name is required');
                    return;
                }
                const companyIds = companyMergeState.ids.slice();
                companyMergeState.inFlight = true;
                setCompanyMergeDialogBusy(true);
                try {
                    const response = await fetch(apiUrl('/api/company/merge'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_ids: companyIds,
                            display_name: trimmed,
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    closeCompanyMergeModal({ force: true });
                    if (typeof waitForRebuildThenReload === 'function') {
                        await waitForRebuildThenReload('companies');
                    } else {
                        window.location.reload();
                    }
                } catch (err) {
                    if (companyMergeState) {
                        companyMergeState.inFlight = false;
                    }
                    setCompanyMergeDialogBusy(false);
                    alert(`Failed to merge companies: ${err.message}`);
                    if (typeof updateCompaniesBulkBar === 'function') {
                        updateCompaniesBulkBar();
                    }
                    // Keep bulk Merge disabled while the dialog is still open.
                    if (companyMergeState) {
                        const mergeBtn = document.getElementById('btn-merge-companies');
                        if (mergeBtn) mergeBtn.disabled = true;
                    }
                }
            }

            (function initCompanyMergeModal() {
                const cancel = document.getElementById('company-merge-cancel');
                const confirmBtn = document.getElementById('company-merge-confirm');
                const modal = document.getElementById('company-merge-modal');
                if (cancel) {
                    cancel.addEventListener('click', closeCompanyMergeModal);
                }
                if (confirmBtn) {
                    confirmBtn.addEventListener('click', confirmCompanyMerge);
                }
                if (modal) {
                    modal.addEventListener('click', (event) => {
                        if (event.target.closest('[data-company-merge-dismiss]')) {
                            closeCompanyMergeModal();
                        }
                    });
                }
            })();
