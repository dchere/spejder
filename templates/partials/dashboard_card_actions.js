            async function setApplied(jobId, applied, inputEl) {
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {
                    const response = await fetch(apiUrl('/api/applied'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, applied })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (card) {
                        const relevantCheckbox = card.querySelector('.relevant-wrap input');
                        const viewedCheckbox = card.querySelector('.viewed-wrap input');
                        const interviewCheckbox = card.querySelector('.interview-wrap input');
                        const stoppedCheckbox = card.querySelector('.stopped-wrap input');
                        if (applied) {
                            if (relevantCheckbox) relevantCheckbox.checked = true;
                            if (viewedCheckbox) viewedCheckbox.checked = true;
                            const hiddenCheckbox = card.querySelector('.hidden-wrap input');
                            if (hiddenCheckbox) hiddenCheckbox.checked = false;
                            if (appliedStagePanels.includes(card.parentElement)) {
                                moveCardToAppliedStage(card);
                            } else {
                                bumpPanelTotal(card.parentElement, -1);
                                moveCardToAppliedStage(card);
                            }
                            if (typeof waitForRebuildThenReload === 'function') {
                                if (statusEl) statusEl.textContent = 'Saved: applied';
                                refreshCounts();
                                await waitForRebuildThenReload('applied');
                            }
                        } else {
                            if (interviewCheckbox) interviewCheckbox.checked = false;
                            if (stoppedCheckbox) stoppedCheckbox.checked = false;
                            const hiddenCheckbox = card.querySelector('.hidden-wrap input');
                            if (hiddenCheckbox) hiddenCheckbox.checked = false;
                            card.classList.remove('is-soft-filtered');
                            removeCompanyFeedbackUI(card);
                            removeCardFromAppliedStages(card);
                            const stillViewed = Boolean(viewedCheckbox && viewedCheckbox.checked);
                            if (stillViewed) {
                                if (card.parentElement !== panelEditedToday) panelEditedToday.prepend(card);
                            } else {
                                const targetPanel = relevantCheckbox && relevantCheckbox.checked ? panelRelevant : panelNotRelevant;
                                if (card.parentElement !== targetPanel) targetPanel.prepend(card);
                            }
                        }
                    }
                    if (statusEl) statusEl.textContent = `Saved: ${applied ? 'applied' : 'not applied'}`;
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !applied;
                }
            }

            async function setOnInterview(jobId, onInterview, inputEl) {
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {
                    const response = await fetch(apiUrl('/api/interview'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, on_interview: onInterview })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (card) {
                        const stoppedCheckbox = card.querySelector('.stopped-wrap input');
                        if (onInterview && stoppedCheckbox) stoppedCheckbox.checked = false;
                        moveCardToAppliedStage(card);
                        if (onInterview) {
                            if (typeof waitForRebuildThenReload === 'function') {
                                if (statusEl) statusEl.textContent = 'Saved: on interview';
                                refreshCounts();
                                await waitForRebuildThenReload(resolveAppliedStage(card).mode);
                            } else {
                                setMode(resolveAppliedStage(card).mode);
                            }
                        }
                    }
                    if (statusEl) statusEl.textContent = `Saved: ${onInterview ? 'on interview' : 'not on interview'}`;
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !onInterview;
                }
            }

            async function setInterviewStopped(jobId, stopped, inputEl) {
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {
                    let promoteChoice = null;
                    let companyId = null;
                    if (stopped) {
                        let promoteOn = false;
                        try {
                            const statusResp = await fetch(
                                apiUrl(`/api/company/promote-status?job_id=${encodeURIComponent(jobId)}`)
                            );
                            const statusData = await statusResp.json();
                            if (statusResp.ok && statusData.ok) {
                                promoteOn = Boolean(statusData.promote);
                                companyId = statusData.company_id || null;
                            }
                        } catch (_statusErr) {
                            // Fall through: stop without promote dialog if status fails.
                        }
                        if (promoteOn && companyId) {
                            const doStop = window.confirm('Mark this job as stopped?');
                            if (!doStop) {
                                inputEl.checked = !stopped;
                                if (statusEl) statusEl.textContent = '';
                                return;
                            }
                            // OK = keep Promote; Cancel = clear Promote (stop already confirmed).
                            promoteChoice = window.confirm(
                                'Keep promoting this company for the applied-company bonus?\n\nOK = keep · Cancel = clear Promote'
                            );
                        }
                    }
                    const response = await fetch(apiUrl('/api/interview/stopped'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, stopped })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    // Keep (true) is a no-op when already promoted; only POST Clear.
                    let promoteFailedMsg = null;
                    if (promoteChoice === false && companyId) {
                        try {
                            const promoteResp = await fetch(apiUrl('/api/company/promote'), {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    company_id: Number(companyId),
                                    promote: false,
                                }),
                            });
                            const promoteData = await promoteResp.json();
                            if (!promoteResp.ok || !promoteData.ok) {
                                promoteFailedMsg = promoteData.error || 'Promote update failed';
                            }
                        } catch (promoteErr) {
                            promoteFailedMsg = (promoteErr && promoteErr.message)
                                ? promoteErr.message
                                : 'Promote update failed';
                        }
                    }
                    if (card) {
                        const interviewCheckbox = card.querySelector('.interview-wrap input');
                        if (stopped && interviewCheckbox) interviewCheckbox.checked = false;
                        moveCardToAppliedStage(card);
                        if (stopped) {
                            if (typeof waitForRebuildThenReload === 'function') {
                                if (statusEl) {
                                    statusEl.textContent = promoteFailedMsg
                                        ? `Saved: stopped; Promote clear failed: ${promoteFailedMsg}`
                                        : 'Saved: stopped';
                                }
                                refreshCounts();
                                await waitForRebuildThenReload(resolveAppliedStage(card).mode);
                            } else {
                                setMode(resolveAppliedStage(card).mode);
                            }
                        }
                    }
                    if (statusEl) {
                        statusEl.textContent = promoteFailedMsg
                            ? `Saved: ${stopped ? 'stopped' : 'not stopped'}; Promote clear failed: ${promoteFailedMsg}`
                            : `Saved: ${stopped ? 'stopped' : 'not stopped'}`;
                    }
                    refreshCounts();
                } catch (err) {
                    // Revert checkbox only when stop itself failed (promote failures keep Stop checked).
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !stopped;
                }
            }

            async function setHidden(jobId, hidden, inputEl) {
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {
                    const response = await fetch(apiUrl('/api/hidden'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, hidden })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (card) {
                        // Soft-hide only: stay on current stage/category panel; filter via Show hidden.
                        // N is unchanged — soft-hidden still count in the category total.
                        if (typeof applyShowHiddenFilter === 'function') {
                            applyShowHiddenFilter();
                        }
                    }
                    if (statusEl) statusEl.textContent = `Saved: ${hidden ? 'hidden' : 'unhidden'}`;
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !hidden;
                }
            }

            async function deleteJob(jobId, btnEl) {
                const card = btnEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (!window.confirm('Delete this position from the database?')) return;
                try {
                    const response = await fetch(apiUrl('/api/job/delete'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    document.querySelectorAll(`[data-job-id="${jobId}"]`).forEach((el) => {
                        if (
                            el.parentElement === panelRelevant || el.parentElement === panelNotRelevant
                        ) {
                            bumpPanelTotal(el.parentElement, -1);
                        }
                        el.remove();
                    });
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}`;
                }
            }

            async function cleanJobSkills(jobId, triggerEl, options) {
                const card = triggerEl ? triggerEl.closest('.card') : null;
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                const preserveViewed = Boolean(options && options.preserveViewed);
                try {
                    const payload = { job_id: jobId };
                    if (preserveViewed) payload.preserve_viewed = true;
                    const response = await fetch(apiUrl('/api/job/clean-skills'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    document.querySelectorAll(`[data-job-id="${jobId}"]`).forEach((el) => {
                        const tags = el.querySelector('.skill-tags');
                        if (tags) tags.innerHTML = '<span class="skills-empty">No skills extracted</span>';
                        if (!data.viewed_cleared) return;
                        const viewedCheckbox = el.querySelector('.viewed-wrap input');
                        if (viewedCheckbox) viewedCheckbox.checked = false;
                        removeAppliedOnlyUI(el);
                        const relevantCheckbox = el.querySelector('.relevant-wrap input');
                        const targetPanel = relevantCheckbox && relevantCheckbox.checked ? panelRelevant : panelNotRelevant;
                        if (el.parentElement !== targetPanel) {
                            if (
                                el.parentElement === panelRelevant || el.parentElement === panelNotRelevant
                            ) {
                                bumpPanelTotal(el.parentElement, -1);
                            }
                            bumpPanelTotal(targetPanel, 1);
                            targetPanel.prepend(el);
                        }
                    });
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}`;
                }
            }

            async function saveCompanyFeedback(jobId, btnEl) {
                const card = btnEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                const inputEl = card ? card.querySelector('.company-feedback-text') : null;
                const feedback = inputEl ? inputEl.value : '';
                btnEl.disabled = true;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {
                    const response = await fetch(apiUrl('/api/interview/feedback'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, feedback })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (statusEl) statusEl.textContent = 'Saved: company feedback';
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}`;
                } finally {
                    btnEl.disabled = false;
                }
            }
