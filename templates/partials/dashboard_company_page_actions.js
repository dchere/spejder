            function backToFullReport() {
                location.href = apiUrl('/report.html');
            }

            async function renameCompanyOnPage(companyId, displayName, btnEl) {
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
                    location.href = apiUrl('/company.html?company=' + encodeURIComponent(trimmed));
                } catch (err) {
                    alert(`Failed to rename company: ${err.message}`);
                    if (btnEl) btnEl.disabled = false;
                }
            }

            async function deleteCompanyOnPage(companyId, displayName, jobCount, btnEl) {
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
                    location.href = apiUrl('/report.html');
                } catch (err) {
                    alert(`Failed to delete company: ${err.message}`);
                    if (btnEl) btnEl.disabled = false;
                }
            }
