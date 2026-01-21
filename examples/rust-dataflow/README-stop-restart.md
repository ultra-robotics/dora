# Manual node stop/restart example

**dataflow-stop-restart.yml** has three Python nodes in a chain: **producer** → **forwarder** → **run-forever**. Each prints when it runs (`producer sent N`, `forwarder passed N`, `run-forever received N`). You can `dora node stop` and `dora node start` any of them to confirm that any node in the chain can be stopped and restarted.

- **producer**: gets `dora/timer` ticks, sends an incrementing counter.
- **forwarder**: receives from producer, passes data through.
- **run-forever**: receives from forwarder, prints and never exits until STOP.

## Prerequisites

- `dora` on your `PATH` (e.g. `cargo build -p dora-cli` and `target/debug/dora` in `PATH`, or `cargo install --path binaries/cli`).
- Python with `dora-rs` and `pyarrow` (`pip install dora-rs pyarrow`).

## Steps

**1. Start coordinator and daemon**

```bash
dora up
```

**2. Start the dataflow in one terminal (and note the UUID)**

```bash
dora start examples/rust-dataflow/dataflow-stop-restart.yml
```

This attaches and shows logs. You should see `dataflow started: <UUID>` and then lines like `producer sent 1`, `forwarder passed 1`, `run-forever received 1`, etc. Use the `<UUID>` in the other terminal.

**3. In a second terminal: stop and start any node**

```bash
# stop one node (e.g. forwarder)
dora node stop -d <UUID> -n forwarder
# producer and run-forever keep running; run-forever stops receiving until forwarder is back

# start it again
dora node start -d <UUID> -n forwarder
```

Try stopping **producer**, **forwarder**, or **run-forever**; in each case that node’s log line disappears and returns when you `dora node start` it again.

## Tear down

```bash
dora destroy
```

(Or stop the dataflow first with `dora stop -d <UUID>`, then `dora destroy`.)
