# ADR 001: Modular monolith with ports and adapters

Accepted. One package, three entrypoints. Domain depends only on the standard library;
application depends on domain and narrow protocols; adapters implement I/O. This keeps
rule tests deterministic without introducing services or speculative frameworks.
