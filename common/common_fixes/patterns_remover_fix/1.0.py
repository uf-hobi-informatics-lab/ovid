"""Common fix ``patterns_remover_fix`` v1.0 (standalone CLI, launched by ``CommonFuncitons.apply_fix``).

The fix logic lives in ``common/fix_transforms.py`` and is shared with the
single-pass fix chain (``common/fix_chain.py``). This script runs that
transform on the current ``fixed_<table>.csv`` with the legacy CLI
arguments (-f -t -p -ftm -i -o -sr1 -sr2). Diagnostics are written to
``fixer_output/fixers_output/<table>_fixer/<fix label>/`` as before.
"""

from fix_chain import run_legacy_fix_cli


if __name__ == "__main__":
    run_legacy_fix_cli("patterns_remover_fix")
