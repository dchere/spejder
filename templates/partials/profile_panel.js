            const btnProfile = document.getElementById('btn-profile');
            const panelProfile = document.getElementById('panel-profile');
            const profileForm = document.getElementById('profile-form');
            const profileStatus = document.getElementById('profile-status');
            let profileMeta = { fields: [], groups: [] };
            let profileCommitted = null;
            let profileDirty = false;

            function profileHasUnsavedChanges() {
                return profileDirty;
            }

            function markProfileDirty() {
                profileDirty = Object.keys(collectDirtyProfileValues()).length > 0;
                updateProfileStatus();
            }

            function updateProfileStatus() {
                const saveBtn = document.getElementById('btn-profile-save');
                if (saveBtn) saveBtn.disabled = !profileDirty;
                if (!profileStatus) return;
                if (profileDirty) {
                    profileStatus.textContent = 'Unsaved edits';
                    return;
                }
                profileStatus.textContent = profileCommitted ? 'Saved' : '';
            }

            function clearProfileFieldErrors() {
                if (!profileForm) return;
                profileForm.querySelectorAll('.profile-field.invalid').forEach((el) => el.classList.remove('invalid'));
                profileForm.querySelectorAll('.profile-field-error').forEach((el) => el.remove());
            }

            function showProfileFieldErrors(errors) {
                clearProfileFieldErrors();
                if (!errors || !profileForm) return;
                Object.keys(errors).forEach((name) => {
                    const field = profileForm.querySelector(`.profile-field[data-field="${CSS.escape(name)}"]`);
                    if (!field) return;
                    field.classList.add('invalid');
                    const err = document.createElement('div');
                    err.className = 'profile-field-error';
                    err.textContent = errors[name];
                    field.appendChild(err);
                });
            }

            function listStrToTextarea(values) {
                if (!Array.isArray(values)) return '';
                return values.map((v) => String(v)).join('\n');
            }

            function textareaToListStr(text) {
                return String(text || '')
                    .split('\n')
                    .map((line) => line.trim())
                    .filter((line) => line.length > 0);
            }

            function renderSkillPatternRows(container, patterns, readonly) {
                container.innerHTML = '';
                const list = Array.isArray(patterns) ? patterns : [];
                list.forEach((item) => {
                    const row = document.createElement('div');
                    row.className = 'profile-pattern-row';
                    const nameInput = document.createElement('input');
                    nameInput.type = 'text';
                    nameInput.placeholder = 'Name';
                    nameInput.value = item && item.name ? String(item.name) : '';
                    nameInput.disabled = readonly;
                    const patternInput = document.createElement('input');
                    patternInput.type = 'text';
                    patternInput.placeholder = 'Regex pattern';
                    patternInput.value = item && item.pattern ? String(item.pattern) : '';
                    patternInput.disabled = readonly;
                    row.appendChild(nameInput);
                    row.appendChild(patternInput);
                    if (!readonly) {
                        const removeBtn = document.createElement('button');
                        removeBtn.type = 'button';
                        removeBtn.textContent = 'Remove';
                        removeBtn.addEventListener('click', () => {
                            row.remove();
                            markProfileDirty();
                        });
                        row.appendChild(removeBtn);
                        nameInput.addEventListener('input', markProfileDirty);
                        patternInput.addEventListener('input', markProfileDirty);
                    }
                    container.appendChild(row);
                });
                if (!readonly) {
                    const addBtn = document.createElement('button');
                    addBtn.type = 'button';
                    addBtn.className = 'profile-add-pattern';
                    addBtn.textContent = 'Add pattern';
                    addBtn.addEventListener('click', () => {
                        renderSkillPatternRows(container, collectSkillPatterns(container).concat([{ name: '', pattern: '' }]), false);
                        markProfileDirty();
                    });
                    container.appendChild(addBtn);
                }
            }

            function collectSkillPatterns(container) {
                const rows = [];
                container.querySelectorAll('.profile-pattern-row').forEach((row) => {
                    const inputs = row.querySelectorAll('input');
                    if (inputs.length < 2) return;
                    const name = inputs[0].value.trim();
                    const pattern = inputs[1].value.trim();
                    if (!name && !pattern) return;
                    rows.push({ name, pattern });
                });
                return rows;
            }

            function renderProfileForm(values, fields, groups) {
                if (!profileForm) return;
                profileForm.innerHTML = '';
                const fieldsByGroup = {};
                (fields || []).forEach((field) => {
                    const groupId = field.group || 'keywords_scoring';
                    if (!fieldsByGroup[groupId]) fieldsByGroup[groupId] = [];
                    fieldsByGroup[groupId].push(field);
                });
                (groups || []).forEach((group) => {
                    const groupFields = fieldsByGroup[group.id] || [];
                    if (!groupFields.length) return;
                    const section = document.createElement('section');
                    section.className = 'profile-group';
                    section.dataset.group = group.id;
                    const heading = document.createElement('h2');
                    heading.textContent = group.title;
                    section.appendChild(heading);
                    groupFields.forEach((field) => {
                        const wrap = document.createElement('div');
                        wrap.className = 'profile-field';
                        wrap.dataset.field = field.name;
                        wrap.dataset.widget = field.widget;
                        if (field.readonly) wrap.dataset.readonly = '1';
                        const label = document.createElement('div');
                        label.className = 'profile-field-label';
                        label.textContent = field.label || field.name;
                        if (field.readonly) {
                            const tag = document.createElement('span');
                            tag.className = 'readonly-tag';
                            tag.textContent = ' (read-only)';
                            label.appendChild(tag);
                        }
                        wrap.appendChild(label);
                        const value = values ? values[field.name] : undefined;
                        if (field.widget === 'checkbox') {
                            const input = document.createElement('input');
                            input.type = 'checkbox';
                            input.checked = Boolean(value);
                            input.disabled = Boolean(field.readonly);
                            if (!field.readonly) input.addEventListener('change', markProfileDirty);
                            wrap.appendChild(input);
                        } else if (field.widget === 'number') {
                            const input = document.createElement('input');
                            input.type = 'number';
                            input.step = 'any';
                            if (value === null || value === undefined) {
                                input.value = '';
                            } else {
                                input.value = String(value);
                            }
                            input.disabled = Boolean(field.readonly);
                            if (!field.readonly) input.addEventListener('input', markProfileDirty);
                            wrap.appendChild(input);
                        } else if (field.widget === 'list_str') {
                            const input = document.createElement('textarea');
                            input.value = listStrToTextarea(value);
                            input.disabled = Boolean(field.readonly);
                            if (!field.readonly) input.addEventListener('input', markProfileDirty);
                            wrap.appendChild(input);
                        } else if (field.widget === 'skill_patterns') {
                            const container = document.createElement('div');
                            container.className = 'profile-patterns';
                            renderSkillPatternRows(container, value, Boolean(field.readonly));
                            wrap.appendChild(container);
                        } else {
                            const input = document.createElement('input');
                            input.type = 'text';
                            input.value = value === null || value === undefined ? '' : String(value);
                            input.disabled = Boolean(field.readonly);
                            if (!field.readonly) input.addEventListener('input', markProfileDirty);
                            wrap.appendChild(input);
                        }
                        if (field.help) {
                            const help = document.createElement('div');
                            help.className = 'profile-field-help';
                            help.textContent = field.help;
                            wrap.appendChild(help);
                        }
                        section.appendChild(wrap);
                    });
                    profileForm.appendChild(section);
                });
            }

            function normalizeSkillPatterns(value) {
                if (!Array.isArray(value)) return [];
                return value.map((item) => ({
                    name: item && item.name != null ? String(item.name) : '',
                    pattern: item && item.pattern != null ? String(item.pattern) : '',
                }));
            }

            function profileValuesEqual(a, b, widget) {
                if (widget === 'skill_patterns') {
                    return JSON.stringify(normalizeSkillPatterns(a))
                        === JSON.stringify(normalizeSkillPatterns(b));
                }
                return JSON.stringify(a) === JSON.stringify(b);
            }

            function collectDirtyProfileValues() {
                const values = {};
                if (!profileForm) return values;
                const committed = profileCommitted || {};
                profileForm.querySelectorAll('.profile-field').forEach((fieldEl) => {
                    const name = fieldEl.dataset.field;
                    const widget = fieldEl.dataset.widget;
                    if (!name || fieldEl.dataset.readonly === '1') return;
                    let value;
                    if (widget === 'checkbox') {
                        const input = fieldEl.querySelector('input[type="checkbox"]');
                        value = Boolean(input && input.checked);
                    } else if (widget === 'number') {
                        const input = fieldEl.querySelector('input[type="number"]');
                        const raw = input ? input.value.trim() : '';
                        value = raw === '' ? null : Number(raw);
                    } else if (widget === 'list_str') {
                        const input = fieldEl.querySelector('textarea');
                        value = textareaToListStr(input ? input.value : '');
                    } else if (widget === 'skill_patterns') {
                        const container = fieldEl.querySelector('.profile-patterns');
                        value = collectSkillPatterns(container || fieldEl);
                    } else {
                        const input = fieldEl.querySelector('input[type="text"]');
                        value = input ? input.value : '';
                    }
                    if (!profileValuesEqual(value, committed[name], widget)) {
                        values[name] = value;
                    }
                });
                return values;
            }

            async function refreshProfileFromServer() {
                if (!profileForm) return;
                try {
                    const response = await fetch(apiUrl('/api/profile'), { cache: 'no-store' });
                    if (!response.ok) return;
                    const data = await response.json();
                    if (!data.ok) return;
                    profileMeta = { fields: data.fields || [], groups: data.groups || [] };
                    profileCommitted = data.values || {};
                    profileDirty = false;
                    clearProfileFieldErrors();
                    renderProfileForm(profileCommitted, profileMeta.fields, profileMeta.groups);
                    updateProfileStatus();
                } catch (_err) {
                    if (profileStatus) profileStatus.textContent = 'Profile API unavailable';
                }
            }

            async function saveProfile(btnEl) {
                if (!profileForm) return;
                clearProfileFieldErrors();
                const payload = collectDirtyProfileValues();
                if (!Object.keys(payload).length) {
                    profileDirty = false;
                    updateProfileStatus();
                    return;
                }
                btnEl.disabled = true;
                if (profileStatus) profileStatus.textContent = 'Saving…';
                try {
                    const response = await fetch(apiUrl('/api/profile/save'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload),
                    });
                    const data = await response.json();
                    if (!response.ok || !data.ok) {
                        if (data && data.errors) showProfileFieldErrors(data.errors);
                        throw new Error(data.error || 'Save failed');
                    }
                    profileCommitted = data.values || Object.assign({}, profileCommitted || {}, payload);
                    profileDirty = false;
                    renderProfileForm(profileCommitted, profileMeta.fields, profileMeta.groups);
                    updateProfileStatus();
                } catch (err) {
                    updateProfileStatus();
                    alert(`Failed to save profile: ${err.message}`);
                } finally {
                    if (btnEl) btnEl.disabled = !profileDirty;
                }
            }

            if (btnProfile) {
                btnProfile.addEventListener('click', () => setMode('profile'));
            }
            window.saveProfile = saveProfile;
            updateProfileStatus();
