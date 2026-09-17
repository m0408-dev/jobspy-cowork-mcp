"""Admin-only release helper. Keeps existing environment private and retains rollback container."""
import argparse
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

def docker(*args, capture=True):
    result = subprocess.run(["docker", *args], text=True, capture_output=capture, timeout=180)
    if result.returncode:
        # Docker errors may contain secret environment values; do not echo them.
        raise RuntimeError(f"Docker {args[0]} failed (exit {result.returncode}); inspect server logs privately")
    return result.stdout.strip() if capture else ""

def inspect(name):
    return json.loads(docker("inspect", name))[0]

def wait_healthy(name):
    deadline = time.monotonic()+90
    while time.monotonic() < deadline:
        state = inspect(name)["State"]
        if state["Status"] != "running":
            raise RuntimeError(f"{name} is not running")
        if state.get("Health",{}).get("Status") == "healthy":
            return
        time.sleep(2)
    raise RuntimeError(f"{name} did not become healthy")

PROTOCOL = '''import asyncio, json, os
from fastmcp import Client
async def main():
    url='http://127.0.0.1:8000'+os.environ.get('MCP_HTTP_PATH','/mcp/')
    async with Client(url, auth=os.environ.get('MCP_AUTH_TOKEN') or None) as c:
        ts=await c.list_tools()
        assert len(ts)==11
        r=await c.call_tool('list_job_sources',{})
        d=json.loads(r.content[0].text)
        assert d['version']=='3.1.0'
        print('MCP protocol OK: 11 tools, v3.1.0')
asyncio.run(main())'''

def restore(backup):
    if not re.fullmatch(r"jobspy-backup-[a-f0-9]{12}-\d+", backup):
        raise ValueError("Invalid backup name")
    inspect(backup)
    names = docker("ps","-a","--format","{{.Names}}").splitlines()
    if "jobspy" in names:
        docker("stop","-t","30","jobspy")
        docker("rename","jobspy", "jobspy-failed-"+str(int(time.time())))
    docker("rename",backup,"jobspy")
    docker("start","jobspy")
    wait_healthy("jobspy")
    print("Rollback restored previous jobspy container")

def release(image, revision):
    if not re.fullmatch(r"[a-f0-9]{40}",revision): raise ValueError("Full Git revision required")
    if image != f"jobspy-mcp:{revision[:12]}": raise ValueError("Immutable revision tag required")
    old = inspect("jobspy")
    if any(m.get("Type") != "volume" or m.get("Name") != "jobspy-results-v3" or m.get("Destination") != "/app/data" for m in old["Mounts"]):
        raise ValueError("Existing container has mounts: review migration before deploying")
    expected = {"8000/tcp": [{"HostIp":"127.0.0.1", "HostPort":"8000"}]}
    if old["HostConfig"]["PortBindings"] != expected:
        raise ValueError("Unexpected ports; refusing automatic migration")
    label = inspect(image)["Config"].get("Labels",{}).get("org.opencontainers.image.revision")
    if label != revision: raise ValueError("Image revision label mismatch")
    backup = f"jobspy-backup-{revision[:12]}-{int(time.time())}"
    root = Path("/var/lib/jobspy/releases")
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    os.chmod(root,0o700)
    report = root/(backup+".json")
    with os.fdopen(os.open(report,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600),"w") as f:
        json.dump(old,f)
    environment = dict(v.split("=",1) for v in old["Config"]["Env"] if "=" in v)
    environment.update(RESULT_DB="/app/data/results.sqlite",MAX_RESULT_CHARS="24000")
    if any("\n" in v or "\r" in v for v in environment.values()): raise ValueError("Multiline env unsupported")
    fd, env_path = tempfile.mkstemp(prefix="jobspy-env-",dir=root)
    candidate = "jobspy-candidate-"+revision[:12]
    switched = False
    stopped = False
    try:
        with os.fdopen(fd,"w") as f:
            for k,v in environment.items(): f.write(f"{k}={v}\n")
        docker("volume","create","jobspy-results-v3")
        docker("run","--rm","--user","0","--mount","type=volume,src=jobspy-results-v3,dst=/app/data",
               image,"chown","10001:10001","/app/data")
        common = ["--env-file",env_path,"--security-opt","no-new-privileges","--cap-drop","ALL"]
        # Candidate has its own ephemeral snapshot store; no production data writes.
        docker("run","-d","--name",candidate,"-p","127.0.0.1:8001:8000",*common,image)
        wait_healthy(candidate)
        print(docker("exec",candidate,"python","-c",PROTOCOL))
        print("Candidate healthy; switching with retained rollback",flush=True)
        docker("stop","-t","30","jobspy")
        stopped = True
        docker("rename","jobspy",backup)
        switched = True
        restart = old["HostConfig"]["RestartPolicy"]["Name"] or "unless-stopped"
        docker("run","-d","--name","jobspy","--restart",restart,"-p","127.0.0.1:8000:8000",
               "--mount","type=volume,src=jobspy-results-v3,dst=/app/data",*common,image)
        wait_healthy("jobspy")
        print(docker("exec","jobspy","python","-c",PROTOCOL))
        print(json.dumps({"deployed_revision":revision,"rollback_container":backup,"status":"healthy"}))
    except Exception:
        if switched:
            restore(backup)
        elif stopped:
            docker("start","jobspy")
        raise
    finally:
        os.unlink(env_path)
        # Only the disposable candidate container is deleted; previous production is retained.
        if candidate in docker("ps","-a","--format","{{.Names}}").splitlines():
            docker("rm","-f",candidate)

if __name__ == "__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--image");p.add_argument("--revision");p.add_argument("--rollback")
    a=p.parse_args()
    if a.rollback: restore(a.rollback)
    else: release(a.image,a.revision)
