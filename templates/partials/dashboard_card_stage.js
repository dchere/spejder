            function resolveAppliedStage(card) {
                const onInterview = card.querySelector('.interview-wrap input')?.checked;
                const stopped = card.querySelector('.stopped-wrap input')?.checked;
                if (onInterview) return { panel: panelInterview, mode: 'interview' };
                if (stopped) return { panel: panelStopped, mode: 'stopped' };
                return { panel: panelApplied, mode: 'applied' };
            }

            function moveCardToAppliedStage(card) {
                const targetPanel = resolveAppliedStage(card).panel;
                if (card.parentElement !== targetPanel) targetPanel.prepend(card);
                syncAppliedStageUI(card);
            }

            function ensureCompanyFeedbackUI(card) {
                if (!card) return;
                const saved = card.querySelector('.company-feedback-text')?.value
                    || card.getAttribute('data-company-feedback')
                    || '';
                if (card.querySelector('.company-feedback-input')) {
                    const existing = card.querySelector('.company-feedback-text');
                    if (existing && saved && !existing.value) existing.value = saved;
                    return;
                }
                const jobId = card.getAttribute('data-job-id');
                const block = document.createElement('div');
                block.className = 'company-feedback-input';
                block.innerHTML = `
                    <p><strong>Company feedback</strong></p>
                    <textarea class="company-feedback-text" placeholder="Notes from the company..."></textarea>
                    <div><button type="button" class="company-feedback-btn" onclick="saveCompanyFeedback(${jobId}, this)">Save feedback</button></div>
                `.trim();
                const textarea = block.querySelector('.company-feedback-text');
                if (textarea && saved) textarea.value = saved;
                const skillsLine = card.querySelector('.skill-tags')?.closest('p');
                if (skillsLine) {
                    skillsLine.before(block);
                } else {
                    const feedbackDiv = card.querySelector('.feedback');
                    if (feedbackDiv) feedbackDiv.before(block);
                }
            }

            function removeCompanyFeedbackUI(card) {
                card?.querySelector('.company-feedback-input')?.remove();
            }

            function removeAppliedOnlyUI(card) {
                if (!card) return;
                card.querySelectorAll(
                    '.cover-letter-section, .applied-manual-input, .manual-status, '
                    + '.applied-date, .interview-wrap, .stopped-wrap'
                ).forEach((el) => el.remove());
                card.classList.remove('has-applied-date');
            }

            function syncAppliedStageUI(card) {
                if (!card) return;
                const stopped = card.querySelector('.stopped-wrap input')?.checked;
                if (stopped) ensureCompanyFeedbackUI(card);
                else removeCompanyFeedbackUI(card);
            }

            function removeCardFromAppliedStages(card) {
                if (appliedStagePanels.includes(card.parentElement)) card.remove();
            }
