// Shared auth utilities

function getToken() {
    return localStorage.getItem('token');
}

function getUsername() {
    return localStorage.getItem('username');
}

function setAuth(token, username) {
    localStorage.setItem('token', token);
    localStorage.setItem('username', username);
}

function clearAuth() {
    localStorage.removeItem('token');
    localStorage.removeItem('username');
}

function authHeaders() {
    return { 'Authorization': 'Bearer ' + getToken(), 'Content-Type': 'application/json' };
}

function requireAuth() {
    if (!getToken()) {
        window.location.href = '/';
        return false;
    }
    return true;
}

function logout() {
    clearAuth();
    window.location.href = '/';
}

// Login form handler (only on index.html)
const loginForm = document.getElementById('login-form');
if (loginForm) {
    // If already logged in, redirect
    if (getToken()) {
        window.location.href = '/setup.html';
    }

    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const errorEl = document.getElementById('error');
        errorEl.style.display = 'none';
        const btn = document.getElementById('login-btn');
        btn.disabled = true;
        btn.textContent = 'Signing in...';

        try {
            const resp = await fetch('/api/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    username: document.getElementById('username').value,
                    password: document.getElementById('password').value,
                }),
            });
            if (!resp.ok) {
                const data = await resp.json();
                throw new Error(data.detail || 'Login failed');
            }
            const data = await resp.json();
            setAuth(data.token, data.username);
            window.location.href = '/setup.html';
        } catch (err) {
            errorEl.textContent = err.message;
            errorEl.style.display = 'block';
        } finally {
            btn.disabled = false;
            btn.textContent = 'Sign in';
        }
    });
}
