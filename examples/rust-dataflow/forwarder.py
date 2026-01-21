"""Forwarder: passes data through. For stop/start demos. Exits when data value % 10 == 0."""

import sys

from dora import Node


def main():
    node = Node()
    for event in node:
        if event["type"] == "INPUT":
            if event["id"] == "data":
                value = event["value"]
                node.send_output("data", value)
                n = value.to_pylist()[0]
                print(f"forwarder passed {n}", flush=True)
                if n % 10 == 0:
                    print(f"forwarder exiting at {n} (restart_policy will bring it back)", flush=True)
                    sys.exit(1)
        elif event["type"] == "STOP":
            print("forwarder received stop, exiting", flush=True)
            break


if __name__ == "__main__":
    main()
