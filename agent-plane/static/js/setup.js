// Session setup page

if (!requireAuth()) throw new Error('Not authenticated');

const userDisplay = document.getElementById('user-display');
if (userDisplay) userDisplay.textContent = getUsername();

// Set default time range (last 24 hours, UTC via toISOString to match ClickHouse)
const now = new Date();
const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
document.getElementById('end-time').value = now.toISOString().slice(0, 16);
document.getElementById('start-time').value = oneDayAgo.toISOString().slice(0, 16);

// Load available services
async function loadServices() {
    try {
        const resp = await fetch('/api/sessions/services', { headers: authHeaders() });
        if (resp.status === 401) { logout(); return; }
        const data = await resp.json();
        const select = document.getElementById('services');
        select.innerHTML = '';
        for (const svc of data.services) {
            const opt = document.createElement('option');
            opt.value = svc;
            opt.textContent = svc;
            select.appendChild(opt);
        }
    } catch (err) {
        console.error('Failed to load services:', err);
    }
}

// Load existing sessions
async function loadSessions() {
    const container = document.getElementById('sessions-list');
    try {
        const resp = await fetch('/api/sessions', { headers: authHeaders() });
        if (resp.status === 401) { logout(); return; }
        const sessions = await resp.json();

        if (sessions.length === 0) {
            container.innerHTML = '<p style="color:var(--text-muted);font-size:0.875rem;">No sessions yet. Create one above.</p>';
            return;
        }

        container.innerHTML = sessions.map(s => {
            const statusClass = s.status === 'ready' ? 'badge-ready' : s.status === 'building' ? 'badge-building' : 'badge-error';
            const clickable = s.status === 'ready' ? `onclick="openSession('${s.id}')"` : '';
            return `
                <div class="session-card" ${clickable}>
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-weight:500;font-size:0.9375rem;">${s.id}</span>
                        <span class="badge ${statusClass}">${s.status}</span>
                    </div>
                    <div class="session-meta">
                        ${s.signal_types.map(t => `<span class="badge badge-signal">${t}</span>`).join('')}
                    </div>
                    <div style="font-size:0.8125rem;color:var(--text-muted);margin-top:0.5rem;">
                        ${s.start_time.slice(0, 16).replace('T', ' ')} — ${s.end_time.slice(0, 16).replace('T', ' ')}
                        ${s.services.length ? ' | ' + s.services.join(', ') : ' | all services'}
                    </div>
                    ${s.error ? `<div style="font-size:0.8125rem;color:var(--danger);margin-top:0.25rem;">${s.error}</div>` : ''}
                </div>
            `;
        }).join('');
    } catch (err) {
        container.innerHTML = '<p style="color:var(--danger);font-size:0.875rem;">Failed to load sessions</p>';
    }
}

function openSession(id) {
    window.location.href = `/chat.html?session=${id}`;
}

// Create session
document.getElementById('create-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const errorEl = document.getElementById('create-error');
    errorEl.style.display = 'none';
    const btn = document.getElementById('create-btn');
    btn.disabled = true;
    btn.textContent = 'Creating...';

    const services = Array.from(document.getElementById('services').selectedOptions).map(o => o.value);
    const signalTypes = Array.from(document.querySelectorAll('input[type=checkbox]:checked')).map(c => c.value);
    const startTime = document.getElementById('start-time').value;
    const endTime = document.getElementById('end-time').value;

    try {
        const resp = await fetch('/api/sessions', {
            method: 'POST',
            headers: authHeaders(),
            body: JSON.stringify({
                services,
                signal_types: signalTypes,
                start_time: startTime,
                end_time: endTime,
            }),
        });
        if (!resp.ok) {
            const data = await resp.json();
            throw new Error(data.detail || 'Failed to create session');
        }
        // Reload sessions list and poll for status
        loadSessions();
        pollSessions();
    } catch (err) {
        errorEl.textContent = err.message;
        errorEl.style.display = 'block';
    } finally {
        btn.disabled = false;
        btn.textContent = 'Create Session';
    }
});

// Poll for building sessions
let pollTimer = null;
function pollSessions() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(async () => {
        await loadSessions();
        // Stop polling if no building sessions
        const resp = await fetch('/api/sessions', { headers: authHeaders() });
        if (resp.ok) {
            const sessions = await resp.json();
            if (!sessions.some(s => s.status === 'building')) {
                clearInterval(pollTimer);
                pollTimer = null;
            }
        }
    }, 3000);
}

// Init
loadServices();
loadSessions();
