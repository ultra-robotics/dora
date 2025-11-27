""" 
# dora-rs.

This is the dora python client for interacting with dora dataflow.
You can install it via:
```bash
pip install dora-rs
```.
"""

from enum import Enum

# Import everything from the Rust module
from .dora import (
    Ros2Context,
    Ros2Durability,
    Ros2Liveliness,
    Ros2Node,
    Ros2NodeOptions,
    Ros2Publisher,
    Ros2QosPolicies,
    Ros2Subscription,
    Ros2Topic,
    __author__,
    __version__,
    start_runtime,
)

# Try to import the new separate Node/Events API
try:
    from .dora import Events, init_node
    _HAS_SEPARATE_API = True
except ImportError:
    # Fallback for older versions that don't have Events/init_node yet
    _HAS_SEPARATE_API = False
    # We'll define a compatibility shim below

# Import the raw Node class (will be the Rust Node if separate API exists,
# or the old combined Node if not)
try:
    from .dora import Node as _RustNode
except ImportError:
    _RustNode = None


if _HAS_SEPARATE_API:
    # New API: Node and Events are separate
    class Node:
        """Compatibility wrapper for Node that preserves the existing API.
        
        This class wraps the separate Node and Events objects to maintain
        backward compatibility with the existing API while allowing separate
        access to Node and Events if needed.
        
        Usage:
            ```python
            from dora import Node
            
            node = Node()  # Works exactly as before
            event = node.next()  # Delegates to events.next()
            for event in node:  # Delegates to events iteration
                pass
            ```
        """
        
        def __init__(self, node_id=None):
            """Initialize a Node and Events pair.
            
            Args:
                node_id: Optional node ID string. If None, uses environment variables.
            """
            rust_node, self._events = init_node(node_id)
            # Store the Rust node internally
            self._node = rust_node
        
        def next(self, timeout=None):
            """Get the next event. Delegates to events.next()."""
            return self._events.next(timeout)
        
        def drain(self):
            """Drain all available events. Delegates to events.drain()."""
            return self._events.drain()
        
        async def recv_async(self, timeout=None):
            """Receive event asynchronously. Delegates to events.recv_async()."""
            return await self._events.recv_async(timeout)
        
        def __iter__(self):
            """Make Node iterable. Delegates to events iteration."""
            return iter(self._events)
        
        def __next__(self):
            """Get next event in iteration. Delegates to events.__next__()."""
            return next(self._events)
        
        def events(self):
            """Return the Events object.
            
            Returns:
                The Events object for this node.
            """
            return self._events
        
        def merge_external_events(self, subscription):
            """Merge external event stream. Delegates to events.merge_external_events()."""
            return self._events.merge_external_events(subscription)
        
        # Delegate node-specific methods to the underlying node
        def send_output(self, output_id, data, metadata=None):
            """Send output from the node."""
            return self._node.send_output(output_id, data, metadata)
        
        def dataflow_descriptor(self):
            """Get the dataflow descriptor."""
            return self._node.dataflow_descriptor()
        
        def node_config(self):
            """Get the node configuration."""
            return self._node.node_config()
        
        def dataflow_id(self):
            """Get the dataflow ID."""
            return self._node.dataflow_id()
        
        def id(self):
            """Get the node ID."""
            return self._node.id()
else:
    # Fallback: Use the old combined Node API
    if _RustNode is not None:
        Node = _RustNode
    else:
        # If neither API is available, create a dummy class
        class Node:
            def __init__(self, *args, **kwargs):
                raise RuntimeError(
                    "Node class not available. Please rebuild the dora module."
                )


# Export Events if available
if _HAS_SEPARATE_API:
    __all__ = [
        "Node",
        "Events",
        "Ros2Context",
        "Ros2Durability",
        "Ros2Liveliness",
        "Ros2Node",
        "Ros2NodeOptions",
        "Ros2Publisher",
        "Ros2QosPolicies",
        "Ros2Subscription",
        "Ros2Topic",
        "__author__",
        "__version__",
        "init_node",
        "start_runtime",
        "DoraStatus",
    ]
else:
    __all__ = [
        "Node",
        "Ros2Context",
        "Ros2Durability",
        "Ros2Liveliness",
        "Ros2Node",
        "Ros2NodeOptions",
        "Ros2Publisher",
        "Ros2QosPolicies",
        "Ros2Subscription",
        "Ros2Topic",
        "__author__",
        "__version__",
        "start_runtime",
        "DoraStatus",
    ]


class DoraStatus(Enum):
    """Dora status to indicate if operator `on_input` loop should be stopped.

    Args:
        Enum (u8): Status signaling to dora operator to
        stop or continue the operator.

    """

    CONTINUE = 0
    STOP = 1
    STOP_ALL = 2
