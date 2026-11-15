#!/usr/bin/env python3
"""Overlay your config's values onto zendrite-sample.yaml, keeping the sample's docs.

Writes a fully documented copy of your config. Your file stays the source of truth
(a small override file); use the merged copy to read, or diff it against a copy made
from an older sample to see which options an upgrade added.

    scripts/merge_yaml.py [-c dendrite.yaml] [-s zendrite-sample.yaml]
                          [-o dendrite.merged.yaml]

The output contains your secrets (it is gitignored as *.merged.yaml).
Needs PyYAML. Options your file sets that the sample lacks are appended as a
commented block at the end and listed on stderr.
"""

import argparse
import json
import os
import re
import sys
import tempfile

import yaml

KEY_RE = re.compile(r"^(\s*)([A-Za-z0-9_.\-]+):(?:\s+(.*?))?\s*$")


def scalar(v):
    """Format a scalar as a JSON literal, which is valid YAML."""
    if v is None:
        return "null"
    try:
        return json.dumps(v, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(v), ensure_ascii=False)


def is_scalar(v):
    """Return whether v is neither a dict nor a list."""
    return not isinstance(v, (dict, list))


def trailing_comment(text):
    """Return a YAML comment suffix outside any quoted scalar, if present."""
    quote = None
    escaped = False
    for i, char in enumerate(text):
        if quote == '"' and escaped:
            escaped = False
            continue
        if quote == '"' and char == "\\":
            escaped = True
            continue
        if quote and char == quote:
            quote = None
        elif not quote and char in "'\"":
            quote = char
        elif not quote and char == "#" and (i == 0 or text[i - 1].isspace()):
            return text[i:]
    return ""


def dump_block(v, indent):
    """Dump v as indented block-style YAML lines."""
    text = yaml.safe_dump(
        v, default_flow_style=False, sort_keys=False, allow_unicode=True
    )
    pad = " " * indent
    return "".join(pad + ln + "\n" for ln in text.splitlines())


def lookup(cfg, path):
    """Return (found, value) for a key path in a nested dict."""
    cur = cfg
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return False, None
        cur = cur[p]
    return True, cur


def leaves(v, path=()):
    """Leaf paths of v; lists and empty dicts count as leaves."""
    if isinstance(v, dict) and v:
        for k, sub in v.items():
            yield from leaves(sub, path + (k,))
    else:
        yield path, v


def merge(sample_lines, user):  # pylint: disable=too-many-locals
    """Overlay user values onto the sample lines; return (lines, used paths)."""
    out, stack, used = [], [], set()
    i, n = 0, len(sample_lines)

    def block_end(start, key_indent):
        """Index after the indented block (comments included) under a key line."""
        j = start
        while j < n:
            ln = sample_lines[j]
            if ln.strip() and not ln.lstrip().startswith("#"):
                line_indent = len(ln) - len(ln.lstrip())
                if line_indent < key_indent or (
                    line_indent == key_indent and not ln.lstrip().startswith("-")
                ):
                    break
            j += 1
        # do not swallow trailing blank/comment lines that belong to the next key
        while j > start and (
            not sample_lines[j - 1].strip()
            or sample_lines[j - 1].lstrip().startswith("#")
        ):
            j -= 1
        return j

    while i < n:
        line = sample_lines[i]
        m = KEY_RE.match(line.rstrip("\n"))
        if not m or line.lstrip().startswith(("#", "-")):
            out.append(line)
            i += 1
            continue
        indent, key, inline = len(m.group(1)), m.group(2), m.group(3) or ""
        comment = trailing_comment(inline)
        if comment and not comment.startswith(" "):
            comment = " " + comment
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = tuple(k for _, k in stack) + (key,)
        has, val = lookup(user, path)
        stack.append((indent, key))
        child_end = block_end(i + 1, indent)
        has_children = child_end > i + 1 and any(
            sample_lines[j].strip() and not sample_lines[j].lstrip().startswith("#")
            for j in range(i + 1, child_end)
        )
        first = next(
            (
                sample_lines[j]
                for j in range(i + 1, child_end)
                if sample_lines[j].strip()
                and not sample_lines[j].lstrip().startswith("#")
            ),
            "",
        )
        sample_is_list = first.lstrip().startswith("-")
        if not has:
            out.append(line)
            if sample_is_list:  # copy the list block verbatim, never parse inside it
                body = slice(i + 1, child_end)
                out.extend(sample_lines[body])
                i = child_end
            else:
                i += 1
            continue
        if is_scalar(val):
            out.append(f"{' ' * indent}{key}: {scalar(val)}{comment}\n")
            used.add(path)
            i = child_end if has_children else i + 1
        elif sample_is_list or (not has_children) or isinstance(val, list) or (isinstance(val, dict) and not val):
            # lists, and dicts where the sample has a scalar/empty value: replace whole
            out.append(
                f"{' ' * indent}{key}:{comment}\n"
                if val
                else f"{' ' * indent}{key}: {'[]' if isinstance(val, list) else '{}'}{comment}\n"
            )
            if val:
                out.append(dump_block(val, indent + 2))
            used.update(p for p, _ in leaves(val, path))
            i = child_end
        else:
            out.append(line)  # dict with sample children: recurse into them
            i += 1
    return out, used


def main():
    """Parse arguments, merge the config into the sample and write the result."""
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("-c", "--config", default="dendrite.yaml")
    ap.add_argument("-s", "--sample", default="zendrite-sample.yaml")
    ap.add_argument("-o", "--output", default="dendrite.merged.yaml")
    a = ap.parse_args()
    with open(a.config, encoding="utf-8") as f:
        user = yaml.safe_load(f) or {}
    with open(a.sample, encoding="utf-8") as f:
        lines = f.readlines()
    out, used = merge(lines, user)
    missing = [
        p
        for p, _ in leaves(user)
        if p not in used and not any(p[:k] in used for k in range(1, len(p)))
    ]
    if missing:
        rest = {}
        for p, v in leaves(user):
            if p in missing:
                cur = rest
                for k in p[:-1]:
                    cur = cur.setdefault(k, {})
                cur[p[-1]] = v
        out.append(
            "\n# --- Set in your config but not in the sample (not applied above) ---\n"
        )
        out.extend(
            "# " + ln + "\n"
            for ln in yaml.safe_dump(rest, sort_keys=False).splitlines()
        )
        print(
            "Not in sample: " + ", ".join(".".join(p) for p in missing), file=sys.stderr
        )
    # The merged config holds secrets: write it to a private temporary file in
    # the output directory, then replace the output atomically, so a failure
    # leaves any existing output untouched and no partial file behind.
    # Resolve a symlinked output so the link's target is updated, not the link.
    dest = os.path.realpath(a.output)
    out_dir = os.path.dirname(dest)
    fd, tmp_path = tempfile.mkstemp(dir=out_dir, prefix=".merge_yaml.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(out)
        try:
            os.chmod(tmp_path, 0o600)
        except OSError:
            pass  # best effort on platforms without POSIX permissions
        os.replace(tmp_path, dest)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    print(
        f"Wrote {a.output}: {len(used)} of your values applied, "
        f"{len(missing)} not in sample"
    )


if __name__ == "__main__":
    main()
