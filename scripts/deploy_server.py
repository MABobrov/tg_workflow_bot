#!/usr/bin/env python3
"""Deploy bot to server via SSH using paramiko."""
import paramiko
import sys
import time

HOST = "46.23.98.118"
USER = "root"
PASS = "WAzoBTgGLrLB0"
DEPLOY_DIR = "/root/tg_workflow_bot"


def run_cmd(client, cmd, timeout=120):
    """Run command and return stdout/stderr."""
    print(f"\n>>> {cmd}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    exit_code = stdout.channel.recv_exit_status()
    if out:
        print(out)
    if err:
        print(f"STDERR: {err}")
    print(f"[exit: {exit_code}]")
    return out, err, exit_code


def main():
    print(f"Connecting to {USER}@{HOST}...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASS, timeout=15)
    print("Connected!\n")

    # 1. Check current state
    run_cmd(client, f"cd {DEPLOY_DIR} && git log --oneline -3")

    # 2. Stash local changes, then pull
    run_cmd(client, f"cd {DEPLOY_DIR} && git stash --include-untracked", timeout=30)
    out, err, code = run_cmd(client, f"cd {DEPLOY_DIR} && git pull origin main", timeout=60)
    if code != 0:
        print(f"\nERROR: git pull failed! Trying reset...")
        run_cmd(client, f"cd {DEPLOY_DIR} && git fetch origin main && git reset --hard origin/main", timeout=30)
        out, err, code = run_cmd(client, f"cd {DEPLOY_DIR} && git log --oneline -1")
        if code != 0:
            print("FATAL: Could not sync with remote!")
            client.close()
            sys.exit(1)

    # 3. Check .env has webhook config
    out, err, code = run_cmd(client, f"grep -c SHEETS_WEBHOOK_SECRET {DEPLOY_DIR}/.env || echo 'MISSING'")
    if "MISSING" in out or out.strip() == "0":
        print("\nAdding webhook config to .env...")
        run_cmd(client, f'echo "\nSHEETS_WEBHOOK_SECRET=77\nWEBHOOK_PORT=8443" >> {DEPLOY_DIR}/.env')

    # 4. Rebuild and restart
    out, err, code = run_cmd(client, f"cd {DEPLOY_DIR} && docker compose up -d --build", timeout=300)
    if code != 0:
        print(f"\nERROR: docker compose failed!")
        client.close()
        sys.exit(1)

    # 5. Wait for container to start
    time.sleep(5)

    # 6. Check container status
    run_cmd(client, f"cd {DEPLOY_DIR} && docker compose ps")

    # 7. Check logs
    run_cmd(client, f"cd {DEPLOY_DIR} && docker compose logs --tail=20")

    # 8. Check health endpoint
    run_cmd(client, "curl -s http://localhost:8443/health || echo 'Health endpoint not responding yet'")

    print("\n=== DEPLOY COMPLETE ===")
    client.close()


if __name__ == "__main__":
    main()
