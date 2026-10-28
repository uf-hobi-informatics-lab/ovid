#!/usr/bin/env python3
"""
Validate that partner templates (or real partner folders) are complete with respect
to the PCORnet mappers in mapping_scripts/pcornet_mappers.

Checks, for each template / partner folder:
  1. Every `partner_dictionaries.<name>` referenced by a mapper is defined in
     the folder's dictionaries.py.
  2. Every upper-case column a mapper reads via col('X') is produced (aliased) by
     the matching <table>_formatter.py, when that formatter exists.
     This is a static check: formatters that pass columns through without an
     explicit .alias()/withColumn() (e.g. select('*') on a native PCORnet file)
     will be reported and must be verified by hand.

Usage:
    python3 ovid_setup/validate_templates.py                      # all templates
    python3 ovid_setup/validate_templates.py partners/my_partner  # one folder

Exit code is non-zero when anything is missing.
"""
import ast
import glob
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPERS = os.path.join(REPO, "mapping_scripts", "pcornet_mappers")

# Columns the mappers create themselves (joins / derived) rather than reading from
# the formatted file.
MAPPER_DERIVED = {
    "lds_address_history": {"FULL_GEOCODE_COUNTY", "GEOCODE_COUNTY", "GEOCODE_STATE", "ZIP5", "ZIP9"},
    "private_address_history": {"FULL_GEOCODE_COUNTY", "GEOCODE_COUNTY", "GEOCODE_STATE", "ZIP5", "ZIP9"},
}


def defined_names(path):
    tree = ast.parse(open(path, encoding="utf-8").read(), path)
    names = set()
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        for t in targets:
            if isinstance(t, ast.Name):
                names.add(t.id)
    return names


def main(folders):
    mappers = sorted(glob.glob(os.path.join(MAPPERS, "*_mapper.py")))
    failed = False
    for folder in folders:
        dict_path = os.path.join(folder, "dictionaries.py")
        if not os.path.exists(dict_path):
            print(f"[SKIP] {folder}: no dictionaries.py")
            continue
        have = defined_names(dict_path)
        problems = []
        for m in mappers:
            table = os.path.basename(m)[: -len("_mapper.py")]
            src = open(m, encoding="utf-8").read()
            for d in sorted(set(re.findall(r"partner_dictionaries\.([A-Za-z0-9_]+)", src)) - have):
                problems.append(f"dictionary  {d}  (used by {table}_mapper.py)")
            fmt = os.path.join(folder, "formatting_scripts", f"{table}_formatter.py")
            if os.path.exists(fmt):
                read = set(re.findall(r"col\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]\s*\)", src))
                fsrc = open(fmt, encoding="utf-8").read()
                made = set(re.findall(r"alias\(\s*['\"]\s*([A-Z][A-Z0-9_]+)\s*['\"]\s*\)", fsrc))
                made |= set(re.findall(r"withColumn\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]", fsrc))
                for c in sorted(read - made - MAPPER_DERIVED.get(table, set())):
                    problems.append(f"column      {c}  (read by {table}_mapper.py, not produced by {table}_formatter.py)")
        name = os.path.relpath(folder, REPO)
        if problems:
            failed = True
            print(f"[FAIL] {name}: {len(problems)} problem(s)")
            for p in problems:
                print("        - " + p)
        else:
            print(f"[ OK ] {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    args = sys.argv[1:] or sorted(glob.glob(os.path.join(REPO, "ovid_setup", "templates", "*")))
    sys.exit(main([os.path.abspath(a) for a in args if os.path.isdir(a)]))
