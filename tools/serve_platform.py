"""Start TraceKV's local Linux monitoring platform."""
import argparse
import platform
import uvicorn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8010)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("Run the monitoring platform in Linux / WSL")
    uvicorn.run("app.main:app", host="127.0.0.1", port=args.port, workers=1)


if __name__ == "__main__":
    main()
