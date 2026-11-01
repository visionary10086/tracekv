"""Read-only environment inventory. Uses only the Python standard library."""
import argparse
import importlib.metadata
import json
import locale
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def decode(raw, encoding=None):
    if encoding:
        return raw.decode(encoding, errors="replace").lstrip("\ufeff")
    if b"\x00" in raw[:200]:
        return raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode(locale.getpreferredencoding(False), errors="replace")


def command(argv, timeout=30, encoding=None):
    try:
        result = subprocess.run(argv, capture_output=True, timeout=timeout)
        return {"command": argv, "returncode": result.returncode,
                "stdout": decode(result.stdout, encoding), "stderr": decode(result.stderr, encoding)}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": argv, "returncode": None, "error": str(exc)}


def collect():
    packages = {}
    for name in ("vllm", "torch", "triton", "nvidia-ml-py"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    checks = {"gpu": command(["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                              "--format=csv,noheader"])}
    if platform.system() == "Windows":
        checks["windows"] = command([
            "powershell.exe", "-NoProfile", "-Command",
            "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(); "
            "Get-CimInstance Win32_OperatingSystem | "
            "Select-Object Caption,Version,OSArchitecture | ConvertTo-Json"])
        checks["memory"] = command([
            "powershell.exe", "-NoProfile", "-Command",
            "Get-CimInstance Win32_ComputerSystem | "
            "Select-Object TotalPhysicalMemory | ConvertTo-Json"])
        checks["wsl"] = command(["wsl.exe", "--list", "--verbose"], encoding="utf-16-le")
    elif platform.system() == "Linux":
        for name, argv in {
            "os": ["cat", "/etc/os-release"], "kernel": ["uname", "-r"],
            "memory": ["free", "-h"],
            "storage": ["lsblk", "-d", "-o", "NAME,ROTA,SIZE,MODEL"],
            "disk_space": ["df", "-h", "."],
            "bpftrace": ["bpftrace", "--version"],
            "block_tracepoints": ["bpftrace", "-l", "tracepoint:block:*"],
        }.items():
            checks[name] = command(argv)
    return {
        "schema_version": 1, "kind": "environment_inventory",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.system(), "release": platform.release(),
        "architecture": platform.machine(), "python": sys.version,
        "python_executable": sys.executable, "packages": packages,
        "vllm_executable": shutil.which("vllm"),
        "btf_vmlinux_exists": Path("/sys/kernel/btf/vmlinux").is_file(),
        "checks": checks,
        "note": "Inventory only; package presence and BTF existence do not prove compatibility or BPF permissions."
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("x", encoding="utf-8") as handle:
        json.dump(collect(), handle, ensure_ascii=False, indent=2)
    print(args.out)


if __name__ == "__main__":
    main()
