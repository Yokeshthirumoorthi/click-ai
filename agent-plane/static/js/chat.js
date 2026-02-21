// Analysis chat page

if (!requireAuth()) throw new Error('Not authenticated');

const params = new URLSearchParams(window.location.search);
const sessionId = params.get('session');
if (!sessionId) {
    window.location.href = '/setup.html';
    throw new Error('No session');
}

const messagesEl = document.getElementById('messages');
const questionInput = document.getElementById('question-input');
const sendBtn = document.getElementById('send-btn');

// Load session info
async function loadSessionInfo() {
    try {
        const resp = await fetch(`/api/sessions/${sessionId}`, { headers: authHeaders() });
        if (resp.status === 401) { logout(); return; }
        if (resp.status === 404) { window.location.href = '/setup.html'; return; }
        const session = await resp.json();

        document.getElementById('session-info').innerHTML = `
            <div class="sidebar-item"><span>ID:</span> ${session.id}</div>
            <div class="sidebar-item"><span>Status:</span> ${session.status}</div>
            <div class="sidebar-item"><span>Time:</span> ${session.start_time.slice(0, 16).replace('T', ' ')}</div>
            <div class="sidebar-item"><span>To:</span> ${session.end_time.slice(0, 16).replace('T', ' ')}</div>
            <div class="sidebar-item"><span>Services:</span> ${session.services.length ? session.services.join(', ') : 'all'}</div>
            <div class="sidebar-item"><span>Signals:</span> ${session.signal_types.join(', ')}</div>
        `;

        const dataInfo = document.getElementById('data-info');
        if (session.manifest) {
            dataInfo.innerHTML = Object.entries(session.manifest).map(([table, info]) =>
                `<div class="sidebar-item">${table}: <strong>${info.row_count.toLocaleString()}</strong> rows</div>`
            ).join('');
        } else {
            dataInfo.innerHTML = '<div class="sidebar-item" style="color:var(--text-muted);">No data loaded</div>';
        }
    } catch (err) {
        console.error('Failed to load session:', err);
    }
}

function addMessage(role, content, sql) {
    const div = document.createElement('div');
    div.className = `message message-${role}`;

    let html = `<div class="message-bubble">${renderMarkdown(content)}</div>`;
    if (sql) {
        html = `<div class="message-bubble">${renderMarkdown(content)}<div class="message-sql">${escapeHtml(sql)}</div></div>`;
    }
    div.innerHTML = html;
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
}

function addLoading() {
    const div = document.createElement('div');
    div.id = 'loading-msg';
    div.className = 'loading-message';
    div.innerHTML = '<div class="spinner"></div> Analyzing...';
    messagesEl.appendChild(div);
    messagesEl.scrollTop = messagesEl.scrollHeight;
}

function removeLoading() {
    const el = document.getElementById('loading-msg');
    if (el) el.remove();
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function renderMarkdown(text) {
    // Simple markdown: tables, bold, code blocks, paragraphs
    let html = escapeHtml(text);

    // Code blocks
    html = html.replace(/```(\w*)\n([\s\S]*?)```/g, '<pre style="background:var(--bg);padding:0.5rem;border-radius:4px;overflow-x:auto;font-size:0.8125rem;">$2</pre>');

    // Inline code
    html = html.replace(/`([^`]+)`/g, '<code style="background:var(--bg);padding:0.125rem 0.25rem;border-radius:3px;font-size:0.8125rem;">$1</code>');

    // Bold
    html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    // Tables
    html = html.replace(/^(\|.+\|)\n(\|[-| :]+\|)\n((?:\|.+\|\n?)*)/gm, (match, header, sep, body) => {
        const headers = header.split('|').filter(c => c.trim()).map(c => `<th>${c.trim()}</th>`).join('');
        const rows = body.trim().split('\n').map(row => {
            const cells = row.split('|').filter(c => c.trim()).map(c => `<td>${c.trim()}</td>`).join('');
            return `<tr>${cells}</tr>`;
        }).join('');
        return `<table><thead><tr>${headers}</tr></thead><tbody>${rows}</tbody></table>`;
    });

    // Paragraphs
    html = html.replace(/\n\n/g, '</p><p>');
    html = html.replace(/\n/g, '<br>');
    html = `<p>${html}</p>`;

    return html;
}

async function sendQuestion() {
    const question = questionInput.value.trim();
    if (!question) return;

    questionInput.value = '';
    sendBtn.disabled = true;
    addMessage('user', question);
    addLoading();

    try {
        const resp = await fetch(`/api/sessions/${sessionId}/ask`, {
            method: 'POST',
            headers: authHeaders(),
            body: JSON.stringify({ question }),
        });
        removeLoading();

        if (resp.status === 401) { logout(); return; }
        if (!resp.ok) {
            const data = await resp.json();
            addMessage('assistant', `Error: ${data.detail || 'Request failed'}`);
            return;
        }

        const data = await resp.json();
        addMessage('assistant', data.formatted, data.sql);
    } catch (err) {
        removeLoading();
        addMessage('assistant', `Error: ${err.message}`);
    } finally {
        sendBtn.disabled = false;
        questionInput.focus();
    }
}

async function deleteSession() {
    if (!confirm('Delete this session and its data?')) return;
    try {
        await fetch(`/api/sessions/${sessionId}`, {
            method: 'DELETE',
            headers: authHeaders(),
        });
        window.location.href = '/setup.html';
    } catch (err) {
        alert('Failed to delete session: ' + err.message);
    }
}

// Enter to send
questionInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendQuestion();
    }
});

// Init
loadSessionInfo();
questionInput.focus();
