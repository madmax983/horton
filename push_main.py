#!/usr/bin/env python3
"""Fast-forward a repo's default branch via the GitHub API.

Usage (run inside a git repo):
    push_main.py <owner/repo> <branch> <message>

Builds a tree from the working tree (`git ls-files`), verifies it matches
the local HEAD tree, creates one commit parented on the remote branch's
current SHA, then moves the branch ref without force. Never touches
uncommitted state: the tree comes from the index/working tree listing.

Auth goes through the stored `custom.github` credential via the
dynamic_credentials surrogate helper; the raw key is never printed.
"""
import base64
import json
import subprocess
import sys

sys.path.insert(0, "/home/hatch/workspace/skills/github/bin")
sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
from gh import api  # noqa: E402


def fail(stage, status, body):
    print(json.dumps({"ok": False, "stage": stage, "status": status,
                      "error": body}))
    return 1


def main(argv):
    if len(argv) != 4:
        print("usage: push_main.py <owner/repo> <branch> <message>",
              file=sys.stderr)
        return 2
    repo, branch, message = argv[1], argv[2], argv[3]

    s, ref = api("GET", f"/repos/{repo}/git/refs/heads/{branch}")
    if s != 200:
        return fail("branch-ref", s, ref)
    base_sha = ref["object"]["sha"]
    print(f"remote {branch} is at {base_sha}", file=sys.stderr)

    listing = subprocess.run(["git", "ls-files", "-s"], capture_output=True,
                             text=True, check=True).stdout
    tree = []
    for line in listing.splitlines():
        meta, path = line.split("\t", 1)
        mode = meta.split()[0]
        with open(path, "rb") as f:
            raw = f.read()
        try:
            tree.append({"path": path, "mode": mode, "type": "blob",
                         "content": raw.decode("utf-8")})
        except UnicodeDecodeError:
            s, blob = api("POST", f"/repos/{repo}/git/blobs",
                          {"content": base64.b64encode(raw).decode(),
                           "encoding": "base64"})
            if s != 201:
                return fail("blob", s, {"path": path, **blob})
            tree.append({"path": path, "mode": mode, "type": "blob",
                         "sha": blob["sha"]})

    s, tree_resp = api("POST", f"/repos/{repo}/git/trees", {"tree": tree})
    if s != 201:
        return fail("tree", s, tree_resp)
    local_tree = subprocess.run(["git", "rev-parse", "HEAD^{tree}"],
                                capture_output=True, text=True,
                                check=True).stdout.strip()
    if tree_resp["sha"] != local_tree:
        print(json.dumps({"ok": False, "stage": "tree-mismatch",
                          "remote": tree_resp["sha"], "local": local_tree}),
              file=sys.stderr)
        return 1
    print(f"tree matches local HEAD tree {local_tree}", file=sys.stderr)

    s, commit = api("POST", f"/repos/{repo}/git/commits",
                    {"message": message, "tree": tree_resp["sha"],
                     "parents": [base_sha]})
    if s != 201:
        return fail("commit", s, commit)

    s, ref = api("PATCH", f"/repos/{repo}/git/refs/heads/{branch}",
                 {"sha": commit["sha"], "force": False})
    if s != 200:
        return fail("ref", s, ref)

    print(json.dumps({"ok": True, "branch": branch,
                      "commit": commit["sha"], "base": base_sha,
                      "html_url": f"https://github.com/{repo}/commit/{commit['sha']}"}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
