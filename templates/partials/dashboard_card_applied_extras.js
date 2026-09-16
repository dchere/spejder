            async function appendAppliedRawText(jobId, btnEl) {
                const card = btnEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                const inputEl = card ? card.querySelector('.raw-append-input') : null;
                const text = inputEl ? inputEl.value : '';
                if (!text || !text.trim()) {
                    if (statusEl) statusEl.textContent = 'Enter description text first';
                    return;
                }

                btnEl.disabled = true;
                if (statusEl) statusEl.textContent = '';
                try {
                    const response = await fetch(apiUrl('/api/applied/raw-text'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, text })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (statusEl) statusEl.textContent = '';
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}`;
                    btnEl.disabled = false;
                }
            }

            async function setCoverLetterRequested(jobId, requested, inputEl) {
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = '';
                try {
                    const response = await fetch(apiUrl('/api/applied/cover-letter/request'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, requested })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    let inputBlock = card ? card.querySelector('.cover-letter-input') : null;
                    if (requested) {
                        if (!inputBlock && card) {
                            const section = card.querySelector('.cover-letter-section');
                            inputBlock = document.createElement('div');
                            inputBlock.className = 'applied-manual-input cover-letter-input';
                            inputBlock.innerHTML = '<textarea class="cover-letter-text" placeholder="Paste cover letter here..."></textarea><div><button type="button" class="cover-letter-btn" onclick="saveCoverLetter(' + jobId + ', this)">Save</button></div>';
                            if (section) {
                                section.appendChild(inputBlock);
                            }
                        }
                    } else if (inputBlock) {
                        inputBlock.remove();
                    }
                    if (statusEl) statusEl.textContent = '';
                } catch (err) {
                    inputEl.checked = !requested;
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                }
            }

            async function saveCoverLetter(jobId, btnEl) {
                const card = btnEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                const inputEl = card ? card.querySelector('.cover-letter-text') : null;
                const text = inputEl ? inputEl.value : '';
                if (!text || !text.trim()) {
                    if (statusEl) statusEl.textContent = 'Enter cover letter text first';
                    return;
                }

                btnEl.disabled = true;
                if (statusEl) statusEl.textContent = '';
                try {
                    const response = await fetch(apiUrl('/api/applied/cover-letter'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ job_id: jobId, text })
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Request failed');
                    }
                    if (statusEl) statusEl.textContent = '';
                } catch (err) {
                    if (statusEl) statusEl.textContent = `Error: ${err.message}. Start: python -m spejder.cli serve-gui`;
                    btnEl.disabled = false;
                }
            }
