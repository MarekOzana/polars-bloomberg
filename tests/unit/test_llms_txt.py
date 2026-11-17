"""docs/llms.txt follows llmstxt.org, and every page and section it links to exists.

The docs are not built in CI, so these tests run the MkDocs hook that writes
llms-full.txt (dev/llms_full.py) without MkDocs: a renamed page or heading fails
here instead of shipping a broken link.
"""

import importlib.util
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_bbg

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
INDEX = (DOCS / "llms.txt").read_text(encoding="utf-8")
MKDOCS_YML = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
SITE_URL = re.search(r"^site_url:\s*(\S+)", MKDOCS_YML, re.MULTILINE)[1]
REPO_URL = re.search(r"^repo_url:\s*(\S+)", MKDOCS_YML, re.MULTILINE)[1]

_spec = importlib.util.spec_from_file_location("llms_full", ROOT / "dev/llms_full.py")
hook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hook)


def test_llms_txt_has_the_llmstxt_org_layout():
    lines = [line for line in INDEX.splitlines() if line.strip()]
    assert lines[0] == "# polars-bloomberg"
    assert lines[1].startswith("> ")
    assert sum(line.startswith("# ") for line in lines) == 1
    sections = [line[3:] for line in lines if line.startswith("## ")]
    assert sections[0] == "Docs"
    assert sections[-1] == "Optional"


def test_llms_txt_links_are_absolute_and_mostly_to_the_docs_site():
    urls = [url for url, _ in hook.index_links(INDEX)]
    assert all(url.startswith("https://") for url in urls)
    assert SITE_URL == "https://marekozana.github.io/polars-bloomberg/"
    assert sum(url.startswith(SITE_URL) for url in urls) > len(urls) / 2


def test_llms_full_has_every_linked_page_and_the_api():
    text = hook.build_llms_full(ROOT, DOCS, SITE_URL, REPO_URL)

    for url, optional in hook.index_links(INDEX):
        expanded = url.startswith(SITE_URL) or url.startswith(f"{REPO_URL}#")
        assert (f"\nSource: {url}\n" in text) == (expanded and not optional), url
    assert "\n::: " not in text  # mkdocstrings directives became text
    assert "### BQuery.bdp\n\n```python\ndef bdp(self," in text
    assert "### BqlResult.combine\n" in text
    assert "## Errors, Warnings and Empty Results\n" in text  # from README.md
    assert "alt.Chart" not in text  # notebook cells the site hides stay out


@pytest.mark.parametrize(
    ("link", "message"),
    [
        ("usage/gone/", "no page"),
        ("usage/bql/#no-such-heading", "no heading #no-such-heading"),
    ],
)
def test_a_link_to_a_missing_page_or_heading_fails(tmp_path, link, message):
    docs = tmp_path / "docs"
    (docs / "usage").mkdir(parents=True)
    (docs / "usage/bql.md").write_text("# BQL\n\n## Screening\n", encoding="utf-8")
    (docs / "llms.txt").write_text(
        f"# x\n\n> y\n\n## Docs\n\n- [Page]({SITE_URL}{link}): z\n", encoding="utf-8"
    )
    with pytest.raises(hook.LlmsTxtError, match=message):
        hook.build_llms_full(tmp_path, docs, SITE_URL, REPO_URL)
