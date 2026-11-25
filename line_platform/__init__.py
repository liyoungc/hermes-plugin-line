"""LINE Messaging API connector.

Two hosts share the same core (journal, identity, group memory, knowledge
base, reply-first sending):

* ``line_platform.standalone`` — a small self-contained webhook service.
* ``line_platform.adapter`` — a Hermes Agent gateway platform plugin.

The Hermes adapter is imported lazily so the core works without Hermes.
"""


def register(ctx) -> None:
    from .adapter import register as _register

    _register(ctx)


__all__ = ["register"]
