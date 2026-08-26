"""Gating for the tests that need something this repo does not contain.

Each gated marker names a flag, and the flag is the only thing that selects it. The alternative —
letting a test probe for what it needs and skip when it is missing — reports a green suite in the
state hardest to tell apart from a passing one, which is the state where nothing ran at all.
"""

GATED_MARKERS = {
    'integration': '--run-integration',
    'local_stack': '--run-local-stack',
}
"""Marker name to the flag that selects it. A marker absent from here is not gated."""


def pytest_addoption(parser):
    """Register the flags the gated markers name.

    Args:
        parser: Pytest's option parser.
    """
    parser.addoption(
        '--run-integration',
        action='store_true',
        default=False,
        help='Run the tests marked `integration`. They reach real AWS.',
    )
    parser.addoption(
        '--run-local-stack',
        action='store_true',
        default=False,
        help='Run the tests marked `local_stack`. They need the compose stack under tests/local-stack.',
    )


def pytest_collection_modifyitems(config, items):
    """Deselect every gated test whose flag was not passed.

    Deselected rather than skipped. A skip is what a test reports once it has started and found it
    cannot run, which is the runtime probe this file exists to avoid. Deselection is decided from
    the flag before anything runs.

    Args:
        config: The active pytest configuration, holding the flags as parsed.
        items: Collected tests, narrowed in place to the ones that will run.
    """
    selected = []
    deselected = []
    for item in items:
        gates = [flag for marker, flag in GATED_MARKERS.items() if marker in item.keywords]
        if any(not config.getoption(flag) for flag in gates):
            deselected.append(item)
        else:
            selected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected
