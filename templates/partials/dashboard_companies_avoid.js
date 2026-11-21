            let companyAvoidDialogResolver = null;

            function closeCompanyAvoidModal(choice) {
                const modal = document.getElementById('company-avoid-modal');
                if (modal) {
                    modal.classList.add('hidden');
                    modal.setAttribute('hidden', '');
                    modal.setAttribute('aria-hidden', 'true');
                }
                document.removeEventListener('keydown', onCompanyAvoidKeydown);
                const resolve = companyAvoidDialogResolver;
                companyAvoidDialogResolver = null;
                if (resolve) resolve(choice || 'cancel');
            }

            function onCompanyAvoidKeydown(event) {
                if (event.key !== 'Escape') return;
                event.preventDefault();
                closeCompanyAvoidModal('cancel');
            }

            function openCompanyAvoidDialog(avoidOn) {
                return new Promise((resolve) => {
                    const modal = document.getElementById('company-avoid-modal');
                    const title = document.getElementById('company-avoid-title');
                    const message = document.getElementById('company-avoid-message');
                    const yesBtn = document.getElementById('company-avoid-yes');
                    const noBtn = document.getElementById('company-avoid-no');
                    const cancelBtn = document.getElementById('company-avoid-cancel');
                    if (!modal || !yesBtn || !noBtn || !cancelBtn) {
                        resolve('cancel');
                        return;
                    }
                    companyAvoidDialogResolver = resolve;
                    if (title) {
                        title.textContent = avoidOn ? 'Turn Avoid on' : 'Turn Avoid off';
                    }
                    if (message) {
                        message.textContent = avoidOn
                            ? 'Yes: set Avoid and mark not-yet-viewed membership jobs as viewed. No: set Avoid only (future ingest still auto-views). Cancel: leave unchanged.'
                            : 'Yes: clear Avoid and unview safe non-pipeline viewed membership jobs (not Applied / Interview / Stopped). No: clear Avoid only. Cancel: leave unchanged.';
                    }
                    const finish = (choice) => () => closeCompanyAvoidModal(choice);
                    yesBtn.onclick = finish('yes');
                    noBtn.onclick = finish('no');
                    cancelBtn.onclick = finish('cancel');
                    modal.querySelectorAll('[data-company-avoid-dismiss]').forEach((el) => {
                        el.onclick = finish('cancel');
                    });
                    modal.classList.remove('hidden');
                    modal.removeAttribute('hidden');
                    modal.setAttribute('aria-hidden', 'false');
                    document.addEventListener('keydown', onCompanyAvoidKeydown);
                    yesBtn.focus();
                });
            }

            async function setCompanyAvoid(companyId, avoid, inputEl) {
                const row = inputEl ? inputEl.closest('tr') : null;
                const prevChecked = !avoid;
                // Revert until Yes/No; Cancel must leave the prior value with no POST.
                if (inputEl) inputEl.checked = prevChecked;
                const choice = await openCompanyAvoidDialog(Boolean(avoid));
                if (choice === 'cancel') return;
                if (inputEl) {
                    inputEl.checked = Boolean(avoid);
                    inputEl.disabled = true;
                }
                try {
                    const response = await fetch(apiUrl('/api/company/avoid'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_id: Number(companyId),
                            avoid: Boolean(avoid),
                            bulk: choice === 'yes',
                        }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (row) {
                        row.dataset.sortAvoid = avoid ? '1' : '0';
                    }
                    if (data.changed && typeof waitForRebuildThenReload === 'function') {
                        await waitForRebuildThenReload('companies');
                    }
                } catch (err) {
                    if (inputEl) inputEl.checked = prevChecked;
                    alert(`Failed to update avoid: ${err.message}`);
                } finally {
                    if (inputEl) inputEl.disabled = false;
                }
            }
