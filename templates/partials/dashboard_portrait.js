            function portraitHasUnsavedChanges() {
                if (!portraitEditor) return false;
                return portraitDraftPending || portraitEditor.value !== portraitCommitted;
            }

            function updatePortraitStatus() {
                if (!portraitStatus || !portraitEditor) return;
                if (portraitDraftPending) {
                    portraitStatus.textContent = 'Review draft — edit if needed, then Save portrait';
                    return;
                }
                if (portraitEditor.value !== portraitCommitted) {
                    portraitStatus.textContent = 'Unsaved edits';
                    return;
                }
                portraitStatus.textContent = portraitCommitted.trim() ? 'Saved' : '';
            }

            async function refreshPortraitFromServer() {
                if (!portraitEditor || portraitDraftPending) return;
                try {
                    const response = await fetch(apiUrl('/api/portrait'), { cache: 'no-store' });
                    if (!response.ok) return;
                    const data = await response.json();
                    if (!data.ok) return;
                    portraitCommitted = data.text || '';
                    portraitEditor.value = portraitCommitted;
                    updatePortraitStatus();
                } catch (_err) {
                    // Static file:// report or server unavailable — keep embedded text.
                }
            }

            async function regeneratePortrait(btnEl) {
                if (!portraitEditor) return;
                if (
                    portraitEditor.value !== portraitCommitted
                    && !portraitDraftPending
                    && !window.confirm('Replace unsaved edits with a new generated draft?')
                ) {
                    return;
                }
                btnEl.disabled = true;
                portraitStatus.textContent = 'Generating…';
                try {
                    const response = await fetch(apiUrl('/api/portrait/generate'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Generation failed');
                    }
                    portraitEditor.value = data.draft || '';
                    portraitDraftPending = true;
                    if (portraitDiff && portraitDiffWrap) {
                        portraitDiff.innerHTML = data.diff_html || '';
                        portraitDiffWrap.classList.remove('hidden');
                    }
                    updatePortraitStatus();
                } catch (err) {
                    portraitStatus.textContent = '';
                    alert(`Failed to generate portrait: ${err.message}`);
                } finally {
                    btnEl.disabled = false;
                }
            }

            async function savePortrait(btnEl) {
                if (!portraitEditor) return;
                btnEl.disabled = true;
                portraitStatus.textContent = 'Saving…';
                try {
                    const response = await fetch(apiUrl('/api/portrait/save'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ text: portraitEditor.value }),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        throw new Error(data.error || 'Save failed');
                    }
                    portraitCommitted = data.text || portraitEditor.value;
                    portraitDraftPending = false;
                    if (portraitDiffWrap) {
                        portraitDiffWrap.classList.add('hidden');
                    }
                    if (portraitDiff) {
                        portraitDiff.innerHTML = '';
                    }
                    portraitStatus.textContent = 'Saved';
                } catch (err) {
                    portraitStatus.textContent = '';
                    alert(`Failed to save portrait: ${err.message}`);
                } finally {
                    btnEl.disabled = false;
                }
            }
