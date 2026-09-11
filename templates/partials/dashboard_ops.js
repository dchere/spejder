            function apiUrl(path) {
                if (window.location.protocol === 'file:') {
                    return `http://127.0.0.1:8765${path}`;
                }
                return path;
            }

            async function reportLastModified() {
                const response = await fetch(apiUrl('/report.html'), { method: 'HEAD', cache: 'no-store' });
                if (!response.ok) {
                    throw new Error('Failed to read report status');
                }
                return response.headers.get('Last-Modified') || '';
            }

            async function initPageReportMtime() {
                const meta = document.querySelector('meta[name="spejder-report-mtime"]');
                pageReportMtime = meta?.getAttribute('content') || '';
                if (!pageReportMtime) {
                    try {
                        pageReportMtime = await reportLastModified();
                    } catch (_err) {
                        // Static file:// report or server unavailable.
                    }
                }
            }

            async function fetchReportStatus() {
                const response = await fetch(apiUrl('/api/report/status'), { cache: 'no-store' });
                if (!response.ok) {
                    throw new Error('Failed to read report status');
                }
                return response.json();
            }

            function reloadWithTab(mode) {
                const url = new URL(window.location.href);
                url.searchParams.set('tab', mode);
                window.location.href = url.toString();
            }

            function sleep(ms) {
                return new Promise((resolve) => setTimeout(resolve, ms));
            }

            let syncPollTimer = null;

            async function fetchSyncStatus() {
                const response = await fetch(apiUrl('/api/inbox/sync/status'), { cache: 'no-store' });
                if (!response.ok) {
                    throw new Error('Failed to read sync status');
                }
                return response.json();
            }

            function applySyncStatus(data, btnEl, statusEl) {
                const running = Boolean(data.running);
                btnEl.disabled = running;
                if (running) {
                    statusEl.textContent = data.stage_message || data.message || 'Syncing…';
                    return;
                }
                const status = data.status || 'idle';
                if (status === 'complete') {
                    statusEl.textContent = data.message || 'Sync complete — reload the page to see new positions';
                } else if (status === 'skipped') {
                    statusEl.textContent = data.message || 'Nothing to sync — inbox is empty and descriptions are up to date';
                } else if (status === 'failed') {
                    statusEl.textContent = data.message || 'Sync failed';
                } else {
                    statusEl.textContent = '';
                }
            }

            async function pollSyncStatus(btnEl, statusEl) {
                try {
                    const data = await fetchSyncStatus();
                    applySyncStatus(data, btnEl, statusEl);
                    if (data.running) {
                        syncPollTimer = setTimeout(() => pollSyncStatus(btnEl, statusEl), 2000);
                    }
                } catch (err) {
                    statusEl.textContent = `Error: ${err.message}`;
                    btnEl.disabled = false;
                }
            }

            async function syncInbox(btnEl) {
                const statusEl = document.getElementById('sync-inbox-status');
                if (syncPollTimer) {
                    clearTimeout(syncPollTimer);
                    syncPollTimer = null;
                }
                btnEl.disabled = true;
                statusEl.textContent = 'Starting sync…';
                try {
                    const response = await fetch(apiUrl('/api/inbox/sync'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    pollSyncStatus(btnEl, statusEl);
                } catch (err) {
                    statusEl.textContent = `Error: ${err.message}`;
                    btnEl.disabled = false;
                }
            }

            async function initInboxSyncStatus() {
                const btnEl = document.getElementById('btn-sync-inbox');
                const statusEl = document.getElementById('sync-inbox-status');
                if (!btnEl || !statusEl) {
                    return;
                }
                try {
                    const data = await fetchSyncStatus();
                    applySyncStatus(data, btnEl, statusEl);
                    if (data.running) {
                        pollSyncStatus(btnEl, statusEl);
                    }
                } catch (_) {
                    // Status endpoint unavailable when opened as a static file.
                }
            }

            async function regenerateReport(btnEl) {
                const statusEl = document.getElementById('regenerate-status');
                btnEl.disabled = true;
                if (statusEl) statusEl.textContent = 'Regenerating…';
                try {
                    const beforeMtime = await reportLastModified();
                    const response = await fetch(apiUrl('/api/report/rebuild'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    const deadline = Date.now() + 60000;
                    while (Date.now() < deadline) {
                        await sleep(500);
                        const afterMtime = await reportLastModified();
                        if (afterMtime && afterMtime !== beforeMtime) {
                            location.reload();
                            return;
                        }
                    }
                    if (statusEl) {
                        statusEl.textContent = 'Rebuild queued — refresh manually if the page does not update';
                    }
                    btnEl.disabled = false;
                } catch (err) {
                    if (statusEl) {
                        statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    }
                    btnEl.disabled = false;
                }
            }

            function ensureEmptyState(panel) {
                const hasCards = panel.querySelector('.card') !== null;
                let emptyEl = panel.querySelector('.empty');
                if (!hasCards && !emptyEl) {
                    emptyEl = document.createElement('p');
                    emptyEl.className = 'empty';
                    emptyEl.textContent = 'No records found.';
                    panel.appendChild(emptyEl);
                }
                if (hasCards && emptyEl) {
                    emptyEl.remove();
                }
            }

            function bumpPanelTotal(panel, delta) {
                let btn = null;
                if (panel === panelRelevant) btn = btnRelevant;
                if (panel === panelNotRelevant) btn = btnNotRelevant;
                if (!btn) return;
                const current = Number.parseInt(btn.dataset.total || '0', 10) || 0;
                btn.dataset.total = String(Math.max(0, current + delta));
            }

            function refreshCounts() {
                const relevantCount = panelRelevant.querySelectorAll('.card').length;
                const notRelevantCount = panelNotRelevant.querySelectorAll('.card').length;
                const editedTodayCount = panelEditedToday.querySelectorAll('.card').length;
                const appliedCount = panelApplied.querySelectorAll('.card').length;
                const interviewCount = panelInterview.querySelectorAll('.card').length;
                const stoppedCount = panelStopped.querySelectorAll('.card').length;
                const hiddenCount = panelHidden.querySelectorAll('.card').length;
                const skillsCount = panelSkills.querySelectorAll('tbody tr').length;
                const relevantTotal = Number.parseInt(btnRelevant.dataset.total || String(relevantCount), 10) || relevantCount;
                const notRelevantTotal = Number.parseInt(btnNotRelevant.dataset.total || String(notRelevantCount), 10) || notRelevantCount;
                btnRelevant.textContent = `Relevant (${relevantCount}/${relevantTotal})`;
                btnNotRelevant.textContent = `Not relevant (${notRelevantCount}/${notRelevantTotal})`;
                btnEditedToday.textContent = `Edited today (${editedTodayCount})`;
                btnApplied.textContent = `Applied (${appliedCount})`;
                btnInterview.textContent = `Interview (${interviewCount})`;
                btnStopped.textContent = `Stopped (${stoppedCount})`;
                btnHidden.textContent = `Hidden (${hiddenCount})`;
                btnSkills.textContent = `Skills (${skillsCount})`;
                ensureEmptyState(panelRelevant);
                ensureEmptyState(panelNotRelevant);
                ensureEmptyState(panelEditedToday);
                ensureEmptyState(panelApplied);
                ensureEmptyState(panelInterview);
                ensureEmptyState(panelStopped);
                ensureEmptyState(panelHidden);
            }

            async function switchTab(mode) {
                if (mode === currentMode) return;
                if (tabRefreshInProgress) return;
                const tabRefreshStatus = document.getElementById('tab-refresh-status');
                if (tabRefreshStatus) tabRefreshStatus.textContent = '';
                if (!confirmLeaveCurrentPanel(mode)) return;
                if (window.location.protocol === 'file:') {
                    setMode(mode, true);
                    return;
                }
                let status;
                try {
                    status = await fetchReportStatus();
                } catch (_err) {
                    setMode(mode, true);
                    return;
                }
                if (!status.ok) {
                    setMode(mode, true);
                    return;
                }
                if (status.idle && status.last_modified === pageReportMtime) {
                    setMode(mode, true);
                    return;
                }
                if (status.idle && status.last_modified && status.last_modified !== pageReportMtime) {
                    reloadWithTab(mode);
                    return;
                }
                tabRefreshInProgress = true;
                if (tabRefreshStatus) tabRefreshStatus.textContent = 'Refreshing…';
                const deadline = Date.now() + 60000;
                try {
                    while (Date.now() < deadline) {
                        await sleep(500);
                        try {
                            status = await fetchReportStatus();
                        } catch (_err) {
                            continue;
                        }
                        if (status.ok && status.idle && status.last_modified && status.last_modified !== pageReportMtime) {
                            reloadWithTab(mode);
                            return;
                        }
                    }
                    reloadWithTab(mode);
                } finally {
                    tabRefreshInProgress = false;
                }
            }
