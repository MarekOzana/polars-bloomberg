"""Re-run the queries on the recipe pages at a Bloomberg Terminal, paste their output.

    python dev/refresh_recipes.py                            # every docs/recipes page
    python dev/refresh_recipes.py docs/recipes/new-issues.md # one page

A recipe is a ```python block followed by a line that starts with "Output" and a
fenced block. The script runs each recipe on its own, puts what it printed into the
fenced block below it, and sets every "as of YYYY-MM-DD" on the page to today. A
```python block without an Output block below it is left alone.

If any recipe on a page fails, that page is not written. A screen that matches
nothing today fails too (`combine()` on an empty result raises), so try again later
or change the screen. Review `git diff docs/recipes` before committing.
"""

import argparse
import contextlib
import io
import re
import sys
import traceback
from datetime import date
from pathlib import Path

import polars as pl

RECIPES_DIR = Path(__file__).resolve().parents[1] / "docs" / "recipes"

# How the outputs are printed: mkdocs scrolls wide code blocks, and long BQL field
# names such as LATST_STATS_FOR_MOST_RECENT_DEAL stay whole.
TABLE_WIDTH = 160
STRING_LENGTH = 40

# A ```python block, then (after optional blank lines) the "Output..." line and the
# fenced block that holds the output. Neither block may contain a ``` fence, so a
# match never spans two recipes.
RECIPE = re.compile(
    r"(?P<head>```python\n(?P<code>(?:(?!```).)*)```\n"
    r"(?:[ \t]*\n)*Output[^\n]*\n```[^\n]*\n)"
    r"(?:(?!```).)*```",
    re.DOTALL,
)
AS_OF = re.compile(r"(?<=as of )\d{4}-\d{2}-\d{2}")


def main() -> None:
    """Refresh the pages given on the command line, or all recipe pages."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("pages", nargs="*", type=Path, help="markdown recipe pages")
    args = parser.parse_args()
    pages = args.pages or sorted(RECIPES_DIR.glob("*.md"))
    failed = [page for page in pages if not refresh_page(page)]
    if failed:
        sys.exit(f"not written: {', '.join(str(page) for page in failed)}")


def refresh_page(page: Path) -> bool:
    """Re-run every recipe on one page; write the page only if all of them ran."""
    raw = page.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    ok = True
    count = 0

    def rerun(match: re.Match) -> str:
        nonlocal ok, count
        count += 1
        name = f"{page.name} recipe {count}"
        try:
            output = run_recipe(match["code"], name)
        except Exception:  # report every failing recipe, then leave the page alone
            ok = False
            print(f"{name}: FAILED\n{traceback.format_exc()}")
            return match[0]
        print(f"{name}: {output.count(chr(10))} lines")
        return f"{match['head']}{output}```"

    text = RECIPE.sub(rerun, text)
    if count == 0:
        print(f"{page.name}: no recipes found")
    if not ok:
        return False
    text = AS_OF.sub(date.today().isoformat(), text)
    if crlf:
        text = text.replace("\n", "\r\n")
    if text != raw:
        page.write_bytes(text.encode("utf-8"))
    return True


def run_recipe(code: str, name: str) -> str:
    """Run one recipe in a fresh namespace and return what it printed."""
    buffer = io.StringIO()
    with (
        pl.Config(tbl_width_chars=TABLE_WIDTH, fmt_str_lengths=STRING_LENGTH),
        contextlib.redirect_stdout(buffer),
    ):
        exec(compile(code, name, "exec"), {"__name__": "__recipe__"})
    output = buffer.getvalue()
    if not output.strip():
        raise ValueError("the recipe printed nothing")
    return output if output.endswith("\n") else output + "\n"


if __name__ == "__main__":
    main()
