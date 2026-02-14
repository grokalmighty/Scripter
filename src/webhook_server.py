from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from .database import Database
from .webhooks_repo import get_webhook
from .scripts_repo import get_script
from .runs_repo import create_run, finish_run
from .executor import run_command
from .locks import try_acquire, release, owner_id

class WebhookHandler(BaseHTTPRequestHandler):
    db: Database = None
    owner: str = ""

    def _json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")

        if len(parts) == 2 and parts[0] == "trigger":
            name = parts[1]
            wh = get_webhook(self.db, name)
            if wh is None:
                return self._json(404, {"ok": False, "error": "unknown webhook", "name": name})

            script = get_script(self.db, wh["script_id"])
            if script is None:
                return self._json(404, {"ok": False, "error": "script not found"})
            
            lock_key = f"script:{script.id}"
            if not try_acquire(self.db, lock_key, self.owner):
                return self._json(409, {"ok": False, "error": "script is already running"})
            run_id = create_run(self.db, script.id, trigger=f"webhook:{name}")

            try:
                result = run_command(script.command, working_dir=script.working_dir)
                status = "success" if result.exit_code == 0 else "failed"
                finish_run(self.db, run_id, status, result.exit_code, result.stdout, result.stderr)
                return self._json(200, {"ok": True, "run_id": run_id, "status": status})
            except Exception as e:
                finish_run(self.db, run_id, "failed", None, "", f"{type(e).__name__}: {e}")
                return self._json(500, {"ok": False, "run_id": run_id, "error": str(e)})
            finally:
                release(self.db, lock_key, self.owner)
        return self._json(404, {"ok": False, "error": "unknown route"})

def serve(db_path, host: str, port: int):
    db = Database(db_path)
    db.init()

    WebhookHandler.db = db
    WebhookHandler
    server = HTTPServer((host, port), WebhookHandler)
    print(f"Webhook server listening on http://{host}:{port}")
    server.serve_forever()