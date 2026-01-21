"""Run-forever node: prints received data. Exits on STOP or when data value % 10 == 0."""

import sys

from dora import Node


def main():
    node = Node()
    for event in node:
        if event["type"] == "INPUT":
            if event["id"] == "data":
                n = event["value"].to_pylist()[0]
                print(f"run-forever received {n}", flush=True)
                if n % 10 == 0:
                    print(f"run-forever exiting at {n} (restart_policy will bring it back)", flush=True)
                    sys.exit(1)
        elif event["type"] == "STOP":
            print("run-forever received stop, exiting", flush=True)
            break


if __name__ == "__main__":
    main()
