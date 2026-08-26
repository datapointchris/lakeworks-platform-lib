"""The platform team's shared library.

Everything here is consumed by domain pipelines and owned by the platform team. The charter's rule
applies to this package specifically: nothing in it may name a domain or special-case a tenant. A
domain that needs behaviour this library does not have either gets a general capability added here,
or solves it inside its own directory.
"""
