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
                        } else {
                            if (interviewCheckbox) interviewCheckbox.checked = false;
                            if (stoppedCheckbox) stoppedCheckbox.checked = false;
                            removeCompanyFeedbackUI(card);
                            removeCardFromAppliedStages(card);
                            const stillViewed = Boolean(viewedCheckbox && viewedCheckbox.checked);
                            if (stillViewed) {
                                if (card.parentElement !== panelEditedToday) panelEditedToday.prepend(card);
                            } else {
                                const targetPanel = relevantCheckbox && relevantCheckbox.checked ? panelRelevant : panelNotRelevant;
                                if (card.parentElement !== targetPanel) targetPanel.prepend(card);
                                setMode(relevantCheckbox && relevantCheckbox.checked ? 'relevant' : 'not relevant');
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
                        setMode(resolveAppliedStage(card).mode);
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
                    const response = await fetch(apiUrl('/api/interview/stopped'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, stopped })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (card) {
                        const interviewCheckbox = card.querySelector('.interview-wrap input');
                        if (stopped && interviewCheckbox) interviewCheckbox.checked = false;
                        moveCardToAppliedStage(card);
                        setMode(resolveAppliedStage(card).mode);
                    }
                    if (statusEl) statusEl.textContent = `Saved: ${stopped ? 'stopped' : 'not stopped'}`;
                    refreshCounts();
                } catch (err) {
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
                        const viewedCheckbox = card.querySelector('.viewed-wrap input');
                        const appliedCheckbox = card.querySelector('.applied-wrap input');
                        const interviewCheckbox = card.querySelector('.interview-wrap input');
                        const stoppedCheckbox = card.querySelector('.stopped-wrap input');
                        const relevantCheckbox = card.querySelector('.relevant-wrap input');
                        if (hidden) {
                            if (viewedCheckbox) viewedCheckbox.checked = false;
                            if (appliedCheckbox) appliedCheckbox.checked = false;
                            if (interviewCheckbox) interviewCheckbox.checked = false;
                            if (stoppedCheckbox) stoppedCheckbox.checked = false;
                            removeCompanyFeedbackUI(card);
                            removeAppliedOnlyUI(card);
                            if (card.parentElement === panelRelevant || card.parentElement === panelNotRelevant) {
                                bumpPanelTotal(card.parentElement, -1);
                            }
                            if (card.parentElement !== panelHidden) panelHidden.prepend(card);
                        } else {
                            const targetPanel = relevantCheckbox && relevantCheckbox.checked ? panelRelevant : panelNotRelevant;
                            bumpPanelTotal(targetPanel, 1);
                            if (card.parentElement !== targetPanel) targetPanel.prepend(card);
                        }
                    }
                    if (statusEl) statusEl.textContent = `Saved: ${hidden ? 'hidden' : 'unhidden'}`;
                    refreshCounts();
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !hidden;
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
