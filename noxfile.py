"""Nox configuration file for managing test sessions and code quality checks."""

import nox


@nox.session(python=["3.12", "3.13", "3.14"])
def tests(session):
    """Run the tests with pytest across supported Python versions.

    This session performs the following steps:
    1. Installs dependencies from requirements.txt.
    2. Installs pytest and pytest-cov for running tests and generating code coverage.
    3. Installs the package itself.
    4. Runs pytest on the 'tests' directory.

    Usage:
    - To run all versions, use: `nox -s tests`
    - To run a specific version, use: `nox -s tests-3.12` (or tests-3.13/tests-3.14)
    """
    # Install dependencies from requirements.txt
    session.install("-r", "requirements.txt")
    session.install("pytest", "pytest-cov", "pyyaml")
    # Install the package itself if needed
    session.install(".")

    # Run pytest on the 'tests' directory
    session.run("pytest", "tests")


@nox.session(python="3.14")
def ruff(session):
    """Run ruff & black code formatter."""
    # Install black, ruff and mypy
    session.install("black", "ruff", "mypy")

    # Run ruff linter
    session.run("ruff", "check", "polars_bloomberg")
    # Run black linter
    session.run("black", "--check", "polars_bloomberg")
    # Run mypy type checker
    # session.run("mypy", "polars_bloomberg")


@nox.session(python="3.14")
def build(session):
    """Build source and wheel distributions of the package."""
    session.install("build")
    session.run("python", "-m", "build")
