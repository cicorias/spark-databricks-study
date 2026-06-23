"""
teach.py — CLI for the FDE interview prep teaching system.

Usage (via mise):
  mise run teach:status
  mise run teach:next
  mise run teach:lesson 01
  mise run teach:done 01
  mise run teach:restart 01
"""

from __future__ import annotations

import re
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent.parent
LESSONS_DIR = ROOT / "teach" / "lessons"
RECORDS_DIR = ROOT / "teach" / "learning-records"
CURRICULUM = ROOT / "teach" / "CURRICULUM.md"

# ── helpers ──────────────────────────────────────────────────────────────────

def lesson_files() -> list[Path]:
    return sorted(LESSONS_DIR.glob("[0-9][0-9]*.html"))

def pad(n: str | int) -> str:
    return f"{int(n):02d}"

def lesson_num(path: Path) -> str:
    return re.match(r"(\d+)", path.name).group(1).zfill(2)

def record_for(padded: str) -> Path | None:
    matches = sorted(RECORDS_DIR.glob(f"*-lesson-{padded}-*.md"))
    return matches[-1] if matches else None  # newest record wins

def is_complete(padded: str) -> bool:
    rec = record_for(padded)
    if rec is None:
        return False
    return "in-progress" not in rec.read_text()

def is_started(padded: str) -> bool:
    rec = record_for(padded)
    if rec is None:
        return False
    return "in-progress" in rec.read_text()

def curriculum_rows() -> list[tuple[str, str]]:
    """Return [(padded_num, title)] from CURRICULUM.md table rows."""
    rows = []
    for line in CURRICULUM.read_text().splitlines():
        m = re.match(r"\|\s*(\d+)\s*\|\s*([^|]+?)\s*\|", line)
        if m:
            rows.append((pad(m.group(1)), m.group(2).strip()))
    return rows

def open_file(path: Path) -> None:
    import platform, shutil
    rel = path.relative_to(ROOT)
    print(f"  Opening: {rel}")

    system = platform.system()
    # Detect WSL
    is_wsl = system == "Linux" and Path("/proc/version").exists() and (
        "microsoft" in Path("/proc/version").read_text().lower()
    )

    if is_wsl:
        # Prefer wslview (wslu), fall back to explorer.exe with Windows path
        if shutil.which("wslview"):
            subprocess.run(["wslview", str(path)])
        else:
            win_path = subprocess.check_output(["wslpath", "-w", str(path)]).decode().strip()
            subprocess.run(["explorer.exe", win_path])
    elif system == "Darwin":
        subprocess.run(["open", str(path)])
    elif shutil.which("xdg-open"):
        subprocess.run(["xdg-open", str(path)])
    else:
        print(f"  → Open manually: {path}")

def next_record_num() -> str:
    existing = sorted(RECORDS_DIR.glob("*.md"))
    nums = [int(re.match(r"(\d+)", p.name).group(1)) for p in existing if re.match(r"\d+", p.name)]
    return f"{(max(nums, default=0) + 1):04d}"


def mark_started(padded: str, html: Path) -> None:
    """Write an in-progress learning record if not already started or complete."""
    if is_complete(padded) or is_started(padded):
        return
    text = html.read_text()
    title_m = re.search(r"<title>([^<]+)</title>", text)
    title = re.sub(r"Lesson \d+ [—–-] ", "", title_m.group(1)) if title_m else html.stem
    slug = re.sub(r"^\d+-", "", html.stem)
    recnum = next_record_num()
    recfile = RECORDS_DIR / f"{recnum}-lesson-{padded}-{slug}.md"
    today = date.today().isoformat()
    recfile.write_text(
        f"# Lesson {padded} in-progress: {title}\n\n"
        f"Started on {today}.\n\n"
        f"**Status:** in-progress\n\n"
        f"**Implications:** lesson {padded} has been opened. "
        f"Run `mise run teach:done {padded}` when complete.\n"
    )
    print(f"  Marked lesson {padded} as started.")


# ── commands ─────────────────────────────────────────────────────────────────

def cmd_status() -> None:
    rows = curriculum_rows()
    print()
    print("  Databricks FDE Interview Prep — Lesson Progress")
    print("  ================================================")
    for padded, title in rows:
        if is_complete(padded):
            icon = "✅ complete    "
        elif is_started(padded):
            icon = "🟡 started     "
        else:
            icon = "⬜ not started "
        print(f"  {icon}  Lesson {padded} — {title}")
    print()
    total = len(rows)
    done = sum(1 for p, _ in rows if is_complete(p))
    print(f"  Progress: {done}/{total} lessons complete")
    print()
    print("  Commands:")
    print("    mise run teach:next              — open next lesson")
    print("    mise run teach:lesson <N>        — open a specific lesson")
    print("    mise run teach:done <N>          — mark lesson complete + write learning record")
    print("    mise run teach:restart <N>       — reset lesson to not-started")
    print()


def cmd_next() -> None:
    for html in lesson_files():
        padded = lesson_num(html)
        if not is_complete(padded):
            mark_started(padded, html)
            open_file(html)
            return
    print("  🎉 All lessons complete! See teach/CURRICULUM.md for next steps.")


def cmd_lesson(num: str) -> None:
    padded = pad(num)
    matches = sorted(LESSONS_DIR.glob(f"{padded}*.html"))
    if not matches:
        print(f"  No lesson found for number {padded}. Run: mise run teach:status")
        sys.exit(1)
    html = matches[0]
    mark_started(padded, html)
    open_file(html)


def cmd_done(num: str) -> None:
    padded = pad(num)
    matches = sorted(LESSONS_DIR.glob(f"{padded}*.html"))
    if not matches:
        print(f"  No lesson {padded} found. Run: mise run teach:status")
        sys.exit(1)

    html = matches[0]
    # Extract title from <title> tag
    text = html.read_text()
    title_m = re.search(r"<title>([^<]+)</title>", text)
    title = re.sub(r"Lesson \d+ [—–-] ", "", title_m.group(1)) if title_m else html.stem

    slug = re.sub(r"^\d+-", "", html.stem)

    # Remove all prior records for this lesson (started + any stale done records)
    for old in sorted(RECORDS_DIR.glob(f"*-lesson-{padded}-*.md")):
        old.unlink()

    recnum = next_record_num()
    recfile = RECORDS_DIR / f"{recnum}-lesson-{padded}-{slug}.md"
    today = date.today().isoformat()

    recfile.write_text(
        f"# Lesson {padded} complete: {title}\n\n"
        f"Completed on {today}. User worked through the lesson content and passed all knowledge checks.\n\n"
        f"**Implications:** lesson {padded} content is understood and can be skipped in future sessions "
        f"unless the user requests a replay via `mise run teach:restart {padded}`.\n"
    )

    print(f"\n  ✅ Lesson {padded} marked complete.")
    print(f"  Record: {recfile.relative_to(ROOT)}")
    print()
    print("  Save progress across machines:")
    print(f"    git add teach/ && git commit -m 'complete lesson {padded}' && git push")
    print()

    # Show what's next
    remaining = [h for h in lesson_files() if not is_complete(lesson_num(h)) and lesson_num(h) != padded]
    if remaining:
        nxt = lesson_num(remaining[0])
        print(f"  Next up: mise run teach:next   (Lesson {nxt})")
    else:
        print("  🎉 All lessons complete!")
    print()


def cmd_restart(num: str) -> None:
    padded = pad(num)
    files = sorted(RECORDS_DIR.glob(f"*-lesson-{padded}-*.md"))
    if not files:
        print(f"  Lesson {padded} has no learning record — already not started.")
        return
    for f in files:
        f.unlink()
        print(f"  Removed: {f.name}")
    print(f"  Lesson {padded} reset to 'not started'. Run: mise run teach:lesson {padded}")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]
    if not args:
        print("Usage: teach.py <status|next|lesson N|done N|restart N>")
        sys.exit(1)

    cmd = args[0]
    # mise appends extra CLI args after the run string, so filter out empty strings
    extra = [a for a in args[1:] if a]
    arg = extra[0] if extra else "01"

    dispatch = {
        "status": lambda: cmd_status(),
        "next": lambda: cmd_next(),
        "lesson": lambda: cmd_lesson(arg),
        "done": lambda: cmd_done(arg),
        "restart": lambda: cmd_restart(arg),
    }

    if cmd not in dispatch:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

    dispatch[cmd]()


if __name__ == "__main__":
    main()
