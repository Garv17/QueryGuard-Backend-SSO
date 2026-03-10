from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
import logging
import sys

# Internal Imports
from app.api import auth, organizations, users
from app.data_catalog import router as data_catalog_router

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("app")

app = FastAPI(title="QueryGuardAI Backend")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(auth.router)
app.include_router(organizations.router)
app.include_router(users.router)
app.include_router(data_catalog_router)

# --- Root Route (The Dashboard) ---
@app.get("/", response_class=HTMLResponse)
# --- main.py changes ---
@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Dashboard | QueryGuardAI</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-[#0f172a] text-white font-sans min-h-screen flex items-center justify-center">
        <div id="loader" class="text-xl font-bold">Authenticating Session...</div>

        <div id="dashboardUI" class="hidden max-w-4xl w-full mx-4 bg-[#1e293b] p-10 rounded-3xl border border-slate-700 shadow-2xl">
            <div class="flex justify-between items-center mb-10">
                <div>
                    <h1 class="text-4xl font-extrabold text-blue-400">QueryGuardAI</h1>
                    <p class="text-slate-400 mt-2">Enterprise Security Control Center</p>
                </div>
                <div class="text-right">
                    <span id="roleBadge" class="bg-blue-900/50 text-blue-300 px-4 py-1 rounded-full text-sm font-bold border border-blue-700 uppercase tracking-widest">
                        Verifying Role...
                    </span>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-10">
                <div class="bg-[#0f172a] p-6 rounded-2xl border border-slate-800">
                    <p class="text-slate-500 text-xs uppercase font-bold mb-1">SSO Provider</p>
                    <p class="text-white font-bold text-lg">Microsoft Azure AD</p>
                </div>
                <div class="bg-[#0f172a] p-6 rounded-2xl border border-slate-800">
                    <p class="text-slate-500 text-xs uppercase font-bold mb-1">Identity</p>
                    <p id="userEmail" class="text-white font-bold text-sm truncate">Authenticating...</p>
                </div>
            </div>

            <div class="border-t border-slate-800 pt-8 flex justify-between items-center">
                <p class="text-slate-500 text-sm">Active Session: <span class="text-green-400">● Encrypted</span></p>
                <button onclick="handleLogout()" class="bg-red-600 hover:bg-red-500 text-white font-bold py-2 px-8 rounded-xl transition-all">
                    Logout
                </button>
            </div>
        </div>

        <script>
    // 1. IMPROVED TOKEN EXTRACTION
    function getHashToken() {
        const hash = window.location.hash.substring(1); // remove the #
        const params = new URLSearchParams(hash);
        return params.get('access_token');
    }

    const urlToken = getHashToken();
    if (urlToken) {
        console.log("Token found in URL, saving to storage...");
        localStorage.setItem('token', urlToken);
        // Clean the URL immediately to prevent re-processing
        history.replaceState(null, null, window.location.pathname);
    }

    // 2. SESSION CHECK
    const token = localStorage.getItem('token');
    if (!token) {
        console.log("No token found, redirecting to login.");
        window.location.href = '/login';
    } else {
        loadUser(token);
    }

    async function loadUser(activeToken) {
        try {
            const res = await fetch('/auth/me', {
                headers: { 
                    'Authorization': `Bearer ${activeToken}`,
                    'Content-Type': 'application/json'
                }
            });
            
            if (res.ok) {
                const data = await res.json();
                document.getElementById('loader').classList.add('hidden');
                document.getElementById('dashboardUI').classList.remove('hidden');
                document.getElementById('roleBadge').innerText = data.role;
                document.getElementById('userEmail').innerText = data.email;
            } else {
                console.error("Auth/me failed with status:", res.status);
                handleLogout();
            }
        } catch (e) {
            console.error("Fetch error:", e);
            handleLogout();
        }
    }

    function handleLogout() {
        localStorage.removeItem('token');
        window.location.href = '/login';
    }
</script>
    </body>
    </html>
    """

# --- Login UI ---
@app.get("/login", response_class=HTMLResponse)
def login_page():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Login | QueryGuardAI</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { background: #0f172a; display: flex; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; }
            .glass { background: rgba(255, 255, 255, 0.05); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.1); padding: 40px; border-radius: 24px; width: 400px; text-align: center; color: white; }
        </style>
    </head>
    <body>
        <div class="glass">
            <h1 class="text-3xl font-bold text-blue-400 mb-8">QueryGuardAI</h1>
            
            <button onclick="startSSO()" id="ssoBtn" class="w-full bg-white text-black font-bold py-3 rounded-xl mb-6 flex items-center justify-center gap-2 hover:bg-gray-200 transition-all">
                <img src="https://authjs.dev/img/providers/azure.svg" width="20">
                <span id="ssoText">Sign in with Microsoft</span>
            </button>

            <div class="text-gray-500 text-xs uppercase mb-6 tracking-widest border-t border-gray-800 pt-4">Admin Credentials</div>

            <form id="pwForm" class="space-y-4">
                <input type="text" id="user" placeholder="Username" class="w-full p-3 bg-slate-900 border border-slate-700 rounded-lg text-white outline-none focus:border-blue-500">
                <input type="password" id="pass" placeholder="Password" class="w-full p-3 bg-slate-900 border border-slate-700 rounded-lg text-white outline-none focus:border-blue-500">
                <button type="submit" class="w-full bg-blue-600 py-3 rounded-lg text-white font-bold hover:bg-blue-500 transition-all">Login</button>
            </form>
            <p id="err" class="text-red-400 mt-4 text-sm hidden bg-red-900/20 py-2 rounded"></p>
        </div>

        <script>
            async function startSSO() {
                const btn = document.getElementById('ssoBtn');
                const txt = document.getElementById('ssoText');
                txt.innerText = "Connecting to Azure...";
                btn.disabled = true;

                try {
                    const res = await fetch('/auth/sso/login');
                    const data = await res.json();
                    if(data.login_url) {
                        window.location.href = data.login_url;
                    }
                } catch (e) {
                    alert("Backend Unreachable.");
                }
            }

            document.getElementById('pwForm').onsubmit = async (e) => {
                e.preventDefault();
                const res = await fetch('/auth/login', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        username: document.getElementById('user').value, 
                        password: document.getElementById('pass').value
                    })
                });
                const data = await res.json();
                if(res.ok) {
                    localStorage.setItem('token', data.access_token);
                    window.location.href = '/';
                } else {
                    document.getElementById('err').innerText = data.detail;
                    document.getElementById('err').classList.remove('hidden');
                }
            }
        </script>
    </body>
    </html>
    """

@app.get("/health")
async def health():
    return {"status": "ok"}