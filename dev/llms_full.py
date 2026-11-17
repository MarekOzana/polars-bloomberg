"""MkDocs hook that writes llms-full.txt next to llms.txt at every build.

docs/llms.txt is the hand-written index for coding agents (https://llmstxt.org/).
This hook writes llms-full.txt to the built site: that index, followed by the full
text of every page it links to outside its "Optional" section, so an agent gets all
of it in one request.

- A link to a docs page gives the page's Markdown source without its front matter;
  a link with an #anchor gives only that section.
- A link to a notebook gives its Markdown and code cells, without outputs and
  without the cells the site hides.
- A link to the GitHub repository with an #anchor gives that section of README.md.
- A `::: package.Name` line, which mkdocstrings renders on the API page, gives the
  signatures and docstrings of that class.

Every link to the docs or to a README section is checked, also in "Optional": a page
or heading that no longer exists fails the build, so llms.txt cannot drift from the
docs. tests/unit/test_llms_txt.py runs the same check without MkDocs.
"""

from __future__ import annotations

import ast
import copy
import json
import re
import unicodedata
from collections.abc import Iterator
from pathlib import Path
from typing import Any

LLMS_TXT = "llms.txt"
LLMS_FULL_TXT = "llms-full.txt"
HIDDEN_CELL_TAGS = {"hide_code"}  # the tags mkdocs.yml removes with mkdocs-jupyter

_LINK = re.compile(r"^\s*-\s*\[[^\]]+\]\((?P<url>[^)\s]+)\)")
_HEADING = re.compile(r"^(?P<hashes>#{1,6})\s+(?P<text>.+?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(?P<fence>`{3,}|~{3,})")
_DIRECTIVE = re.compile(r"^:::\s+(?P<path>[\w.]+)\s*$")
_FRONT_MATTER = re.compile(r"\A---\s*\n.*?\n---\s*\n", re.DOTALL)
_PRIVATE = re.compile(r"_[^_]")  # mkdocstrings' default filter hides these members

_INTRO = (
    "The rest of this file is the full text of the pages linked above, outside the "
    "Optional section, generated from the documentation sources when the site is "
    "built. Each page starts with its Source URL."
)


class LlmsTxtError(ValueError):
    """A link in llms.txt points to a page or a section that does not exist."""


def on_post_build(config: Any) -> None:
    """Write llms-full.txt to the built site (MkDocs event)."""
    from mkdocs.exceptions import PluginError  # noqa: PLC0415 - only MkDocs needs it

    config_file = Path(config.config_file_path)
    try:
        text = build_llms_full(
            root=config_file.parent,
            docs_dir=Path(config.docs_dir),
            site_url=_published_site_url(config_file) or config.site_url or "",
            repo_url=config.repo_url or "",
        )
    except LlmsTxtError as exc:
        raise PluginError(str(exc)) from exc
    Path(config.site_dir, LLMS_FULL_TXT).write_text(text, encoding="utf-8")


def build_llms_full(root: Path, docs_dir: Path, site_url: str, repo_url: str) -> str:
    """Return the text of llms-full.txt: llms.txt, then each page it links to."""
    index_file = docs_dir / LLMS_TXT
    if not index_file.is_file():
        raise LlmsTxtError(f"{index_file} not found")
    index = index_file.read_text(encoding="utf-8")
    parts = [index.strip(), f"---\n\n{_INTRO}"]
    seen: set[str] = set()
    for url, optional in index_links(index):
        text = _resolve(url, root, docs_dir, site_url, repo_url)
        if text is None or optional or url in seen:
            continue
        seen.add(url)
        parts.append(f"---\n\nSource: {url}\n\n{text.strip()}")
    return "\n\n".join(parts) + "\n"


def index_links(index: str) -> list[tuple[str, bool]]:
    """Return (url, in the Optional section) for each list link in llms.txt."""
    links = []
    optional = False
    for _, line in _outside_fences(index.splitlines()):
        if line.startswith("## "):
            optional = line[3:].strip().lower() == "optional"
        elif match := _LINK.match(line):
            links.append((match["url"], optional))
    return links


def _published_site_url(config_file: Path) -> str | None:
    """Return site_url as written in mkdocs.yml; `mkdocs serve` replaces it."""
    found = re.search(
        r"^site_url:\s*(\S+)", config_file.read_text(encoding="utf-8"), re.MULTILINE
    )
    return found[1].strip("'\"") if found else None


def _resolve(
    url: str, root: Path, docs_dir: Path, site_url: str, repo_url: str
) -> str | None:
    """Return the text a link stands for, or None for a link to another site."""
    address, _, anchor = url.partition("#")
    if site_url and address.startswith(site_url):
        path = address[len(site_url) :]
        source = _page_source(path, docs_dir)
        if source is None:
            if (docs_dir / path).is_file() or path == LLMS_FULL_TXT:
                return None  # a file, not a page
            raise LlmsTxtError(
                f"{LLMS_TXT} links to {url}, but no page in {docs_dir} has that URL"
            )
        text = _page_text(source, root)
    elif anchor and repo_url and address.rstrip("/") == repo_url.rstrip("/"):
        source = root / "README.md"
        text = source.read_text(encoding="utf-8")
    else:
        return None
    if not anchor:
        return text
    section = _section(text, anchor)
    if section is None:
        raise LlmsTxtError(
            f"{LLMS_TXT} links to {url}, but {source} has no heading #{anchor}"
        )
    return section


def _page_source(path: str, docs_dir: Path) -> Path | None:
    """Return the source file of the page at a site path such as 'usage/bql/'."""
    stem = re.sub(r"(?:^|/)index$", "", path.removesuffix(".html")).strip("/")
    for name in (f"{stem}/index", stem) if stem else ("index",):
        for suffix in (".md", ".ipynb"):
            candidate = docs_dir / f"{name}{suffix}"
            if candidate.is_file():
                return candidate
    return None


def _page_text(source: Path, root: Path) -> str:
    """Return a page as Markdown: notebooks as cells, API directives as text."""
    if source.suffix == ".ipynb":
        return _notebook_markdown(source)
    text = _FRONT_MATTER.sub("", source.read_text(encoding="utf-8"), count=1)
    return _expand_directives(text, root)


def _notebook_markdown(source: Path) -> str:
    """Return the Markdown and code cells of a notebook, without outputs."""
    parts = []
    for cell in json.loads(source.read_text(encoding="utf-8"))["cells"]:
        text = "".join(cell["source"]).strip()
        if not text or HIDDEN_CELL_TAGS & set(cell.get("metadata", {}).get("tags", [])):
            continue
        if cell["cell_type"] == "markdown":
            parts.append(text)
        elif cell["cell_type"] == "code":
            parts.append(f"```python\n{text}\n```")
    return "\n\n".join(parts)


def _outside_fences(lines: list[str]) -> Iterator[tuple[int, str]]:
    """Yield (index, line) for the lines that are not inside a fenced code block."""
    fence = ""
    for i, line in enumerate(lines):
        if match := _FENCE.match(line):
            marker = match["fence"]
            if not fence:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = ""
        elif not fence:
            yield i, line


def _anchor(heading: str) -> str:
    """Return the anchor of a heading, as Python-Markdown's toc and GitHub make it."""
    if custom := re.search(r"\{[^}]*#([\w-]+)[^}]*\}\s*$", heading):
        return custom[1]
    text = re.sub(r"\{[^}]*\}\s*$", "", heading)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def _section(text: str, anchor: str) -> str | None:
    """Return the section under the heading with this anchor, or None."""
    lines = text.splitlines()
    start, level = -1, 0
    for i, line in _outside_fences(lines):
        if not (match := _HEADING.match(line)):
            continue
        depth = len(match["hashes"])
        if start < 0:
            if _anchor(match["text"]) == anchor:
                start, level = i, depth
        elif depth <= level:
            return "\n".join(lines[start:i])
    return "\n".join(lines[start:]) if start >= 0 else None


def _expand_directives(text: str, root: Path) -> str:
    """Replace each `::: package.Name` line, and its options, with the API text."""
    out = []
    in_options = False
    for line in text.splitlines():
        if match := _DIRECTIVE.match(line):
            out.append(_api_markdown(match["path"], root))
            in_options = True
        elif not (in_options and line[:1] in {" ", "\t"}):
            in_options = False
            out.append(line)
    return "\n".join(out)


def _api_markdown(dotted: str, root: Path) -> str:
    """Return the docstring and the documented public methods of a class."""
    module, _, name = dotted.rpartition(".")
    node = _find_class(module, name, root)
    if node is None:
        raise LlmsTxtError(f"::: {dotted}: no class {name} in {module} under {root}")
    parts = [f"## {dotted}", "", ast.get_docstring(node) or ""]
    for item in node.body:
        if not isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        doc = ast.get_docstring(item)
        if not doc or _PRIVATE.match(item.name):
            continue
        stub = copy.copy(item)
        stub.body = [ast.Expr(ast.Constant(...))]
        signature = ast.unparse(stub)
        parts += ["", f"### {node.name}.{item.name}", "", "```python", signature]
        parts += ["```", "", doc]
    return "\n".join(parts)


def _find_class(module: str, name: str, root: Path) -> ast.ClassDef | None:
    """Find a class in a module's source, following `from .x import Name`."""
    base = root.joinpath(*module.split("."))
    path = next(
        (p for p in (base / "__init__.py", base.with_suffix(".py")) if p.is_file()),
        None,
    )
    if path is None:
        return None
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            if (alias.asname or alias.name) == name:
                source = node.module or ""
                if node.level:
                    parent = package.rsplit(".", node.level - 1)[0]
                    source = ".".join(filter(None, [parent, node.module]))
                return _find_class(source, alias.name, root)
    return None
