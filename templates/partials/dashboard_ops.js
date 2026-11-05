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

            function setSyncStatusText(statusEl, text) {
                const value = text || '';
                statusEl.textContent = value;
                if (value) {
                    statusEl.setAttribute('title', value);
                } else {
                    statusEl.removeAttribute('title');
                }
            }

            const SYNC_RELOAD_GENERATION_KEY = 'spejder-sync-reload-generation';

            function handledSyncReloadGeneration() {
                const raw = sessionStorage.getItem(SYNC_RELOAD_GENERATION_KEY);
                const n = Number.parseInt(raw || '0', 10);
                return Number.isFinite(n) ? n : 0;
            }

            function maybeMidSyncReload(data) {
                const gen = Number.parseInt(String(data.reload_generation ?? 0), 10);
                // Process restart resets server gen to 0 while sessionStorage
                // may still hold a prior value — treat a backward jump as a
                // new epoch so mid-sync reload is not skipped forever.
                let handled = handledSyncReloadGeneration();
                if (Number.isFinite(gen) && gen < handled) {
                    sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, '0');
                    handled = 0;
                }
                if (!Number.isFinite(gen) || gen <= handled) {
                    return false;
                }
                // Persist before navigate so the reloaded page does not loop.
                // When Applied/tab wait owns navigation, mark handled and skip
                // stacking a second reload (rebuild already coalesced).
                sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, String(gen));
                if (tabRefreshInProgress) {
                    return false;
                }
                try {
                    reloadWithTab(currentMode);
                } catch (_err) {
                    location.reload();
                }
                return true;
            }

            function applySyncStatus(data, btnEl, statusEl) {
                const running = Boolean(data.running);
                btnEl.disabled = running;
                if (running) {
                    setSyncStatusText(
                        statusEl,
                        data.stage_message || data.message || 'Syncing…',
                    );
                    return;
                }
                const status = data.status || 'idle';
                if (status === 'complete') {
                    setSyncStatusText(
                        statusEl,
                        data.message || 'Sync complete — reload the page to see new positions',
                    );
                } else if (status === 'skipped') {
                    setSyncStatusText(
                        statusEl,
                        data.message || 'Nothing to sync — inbox is empty and descriptions are up to date',
                    );
                } else if (status === 'failed') {
                    setSyncStatusText(statusEl, data.message || 'Sync failed');
                } else {
                    setSyncStatusText(statusEl, '');
                }
            }

            async function pollSyncStatus(btnEl, statusEl) {
                try {
                    const data = await fetchSyncStatus();
                    applySyncStatus(data, btnEl, statusEl);
                    if (maybeMidSyncReload(data)) {
                        return;
                    }
                    if (data.running) {
                        syncPollTimer = setTimeout(() => pollSyncStatus(btnEl, statusEl), 2000);
                    }
                } catch (err) {
                    setSyncStatusText(statusEl, `Error: ${err.message}`);
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
                setSyncStatusText(statusEl, 'Starting sync…');
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
                    setSyncStatusText(statusEl, `Error: ${err.message}`);
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
                    if (maybeMidSyncReload(data)) {
                        return;
                    }
                    if (data.running) {
                        pollSyncStatus(btnEl, statusEl);
                    }
                } catch (_) {
                    // Status endpoint unavailable when opened as a static file.
                }
            }

            let regenerateIdleTitle = 'Regenerate report';
            let regenerateBusyCount = 0;

            function setRegenerateBusy(busy) {
                const btnEl = document.getElementById('btn-regenerate');
                if (!btnEl) return;
                if (busy) {
                    regenerateBusyCount += 1;
                    if (regenerateBusyCount > 1) return;
                    regenerateIdleTitle = btnEl.getAttribute('title') || 'Regenerate report';
                    btnEl.disabled = true;
                    btnEl.classList.add('is-busy');
                    btnEl.setAttribute('aria-busy', 'true');
                    btnEl.setAttribute('title', 'Regenerating…');
                    return;
                }
                regenerateBusyCount = Math.max(0, regenerateBusyCount - 1);
                if (regenerateBusyCount > 0) return;
                btnEl.classList.remove('is-busy');
                btnEl.removeAttribute('aria-busy');
                btnEl.setAttribute('title', regenerateIdleTitle);
                btnEl.disabled = false;
            }

            async function regenerateReport(btnEl) {
                const statusEl = document.getElementById('regenerate-status');
                setRegenerateBusy(true);
                if (statusEl) statusEl.textContent = '';
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
                    setRegenerateBusy(false);
                } catch (err) {
                    if (statusEl) {
                        statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    }
                    setRegenerateBusy(false);
                }
            }

            function ensureEmptyState(panel) {
                const hasCards = panel.querySelector('.card:not(.is-soft-filtered)') !== null;
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

            function panelSoftHideCounts(panel) {
                let n = 0;
                let k = 0;
                panel.querySelectorAll('.card').forEach((card) => {
                    const hidden = card.querySelector('.hidden-wrap input');
                    if (hidden && hidden.checked) k += 1;
                    else n += 1;
                });
                return { n, k };
            }

            // Digits + fixed spans only — label is a fixed tab name, never user content.
            function formatJobListBadge(label, n, k, total, opts) {
                const safeN = Number(n) || 0;
                const safeK = Number(k) || 0;
                const safeTotal = Number(total) || 0;
                const active = Boolean(opts && opts.active);
                const showHidden = Boolean(opts && opts.showHidden);
                if (safeK === 0) {
                    return `${label} (${safeN}/${safeTotal})`;
                }
                if (active && !showHidden) {
                    return `${label} (${safeN} + <span class="mode-btn-soft-k">${safeK}</span>/${safeTotal})`;
                }
                return `${label} (${safeN + safeK}/${safeTotal})`;
            }

            function refreshCounts() {
                const showHidden = typeof isShowHiddenEnabled === 'function'
                    ? isShowHiddenEnabled()
                    : false;
                const relevant = panelSoftHideCounts(panelRelevant);
                const notRelevant = panelSoftHideCounts(panelNotRelevant);
                const applied = panelSoftHideCounts(panelApplied);
                const interview = panelSoftHideCounts(panelInterview);
                const stopped = panelSoftHideCounts(panelStopped);
                const editedTodayCount = panelEditedToday.querySelectorAll('.card').length;
                const skillsCount = panelSkills.querySelectorAll('tbody tr').length;
                const companiesPanel = document.getElementById('panel-companies');
                const companiesCount = companiesPanel
                    ? companiesPanel.querySelectorAll('tbody tr').length
                    : 0;
                const btnCompanies = document.getElementById('btn-companies');
                const relevantLoaded = relevant.n + relevant.k;
                const notRelevantLoaded = notRelevant.n + notRelevant.k;
                // N = category total incl. soft-hidden; never below cards on the tab.
                let relevantTotal = Number.parseInt(
                    btnRelevant.dataset.total || String(relevantLoaded), 10
                ) || 0;
                let notRelevantTotal = Number.parseInt(
                    btnNotRelevant.dataset.total || String(notRelevantLoaded), 10
                ) || 0;
                relevantTotal = Math.max(relevantTotal, relevantLoaded);
                notRelevantTotal = Math.max(notRelevantTotal, notRelevantLoaded);
                btnRelevant.dataset.total = String(relevantTotal);
                btnNotRelevant.dataset.total = String(notRelevantTotal);
                btnRelevant.innerHTML = formatJobListBadge(
                    'Relevant', relevant.n, relevant.k, relevantTotal,
                    { active: currentMode === 'relevant', showHidden }
                );
                btnNotRelevant.innerHTML = formatJobListBadge(
                    'Not relevant', notRelevant.n, notRelevant.k, notRelevantTotal,
                    { active: currentMode === 'not relevant', showHidden }
                );
                // Applied / Interview / Stopped: single loaded total (n+k), no split badge.
                btnApplied.textContent = `Applied (${applied.n + applied.k})`;
                btnInterview.textContent = `Interview (${interview.n + interview.k})`;
                btnStopped.textContent = `Stopped (${stopped.n + stopped.k})`;
                btnEditedToday.textContent = `Edited today (${editedTodayCount})`;
                btnSkills.textContent = `Skills (${skillsCount})`;
                if (btnCompanies) btnCompanies.textContent = `Companies (${companiesCount})`;
                ensureEmptyState(panelRelevant);
                ensureEmptyState(panelNotRelevant);
                ensureEmptyState(panelEditedToday);
                ensureEmptyState(panelApplied);
                ensureEmptyState(panelInterview);
                ensureEmptyState(panelStopped);
            }

            async function waitForRebuildThenReload(mode) {
                if (tabRefreshInProgress) return;
                if (window.location.protocol === 'file:') {
                    setMode(mode);
                    return;
                }
                try {
                    await fetchReportStatus();
                } catch (_err) {
                    setMode(mode);
                    return;
                }
                tabRefreshInProgress = true;
                setRegenerateBusy(true);
                let reloading = false;
                const deadline = Date.now() + 60000;
                try {
                    while (Date.now() < deadline) {
                        await sleep(500);
                        let status;
                        try {
                            status = await fetchReportStatus();
                        } catch (_err) {
                            continue;
                        }
                        if (status.ok && status.idle && status.last_modified && status.last_modified !== pageReportMtime) {
                            reloading = true;
                            reloadWithTab(mode);
                            return;
                        }
                    }
                    reloading = true;
                    reloadWithTab(mode);
                } finally {
                    if (!reloading) {
                        setRegenerateBusy(false);
                    }
                    tabRefreshInProgress = false;
                }
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
                await waitForRebuildThenReload(mode);
            }
