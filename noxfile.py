"""Nox configuration file for managing test sessions and code quality checks."""

import nox


@nox.session(python=["3.12", "3.13", "3.14", "3.15"])
def tests(session):
    """Run the tests with pytest across supported Python versions.

    Live tests are skipped; to run them too, use: `nox -s tests -- --bbg`

    Usage:
    - To run all versions, use: `nox -s tests`
    - To run a specific version, use: `nox -s tests-3.15`
    """
    session.install("-r", "requirements.txt")
    session.install("pytest", "pytest-cov", "pyyaml")
    session.install(".")
    session.run("pytest", *session.posargs)


@nox.session(python="3.14")
def ruff(session):
    """Lint and check formatting with ruff, as CI does."""
    session.install("ruff")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python="3.14")
def build(session):
    """Build source and wheel distributions of the package."""
    session.install("build")
    session.run("python", "-m", "build")
