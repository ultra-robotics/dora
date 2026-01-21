"""Producer: receives timer ticks, sends an incrementing counter. For stop/start demos."""

import pyarrow as pa

from dora import Node


def main():
    node = Node()
    n = 0
    for event in node:
        if event["type"] == "INPUT":
            if event["id"] == "tick":
                n += 1
                node.send_output("data", pa.array([n]))
                print(f"producer sent {n}", flush=True)
        elif event["type"] == "STOP":
            print("producer received stop, exiting", flush=True)
            break


if __name__ == "__main__":
    main()
