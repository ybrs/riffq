import io
from pathlib import Path


def read_pyproject_text(project_root: Path) -> str:
    pyproject_path = project_root / "pyproject.toml"
    with pyproject_path.open("r", encoding="utf-8") as file_handle:
        return file_handle.read()


def extract_project_section(pyproject_text: str) -> str:
    in_project = False
    collected_lines = []
    for raw_line in io.StringIO(pyproject_text):
        line = raw_line.rstrip("\n")
        if line.strip().startswith("[") and line.strip().endswith("]"):
            in_project = line.strip() == "[project]"
            continue
        if in_project:
            # Stop if another section begins
            if line.strip().startswith("[") and line.strip().endswith("]"):
                break
            collected_lines.append(line)
    return "\n".join(collected_lines)


def test_pyproject_has_short_description_and_readme():
    project_root = Path(__file__).resolve().parent.parent
    py_text = read_pyproject_text(project_root)
    project_section = extract_project_section(py_text)

    expected_description = (
        "description = \"riffq is a toolkit for building PostgreSQL wire-compatible databases. "
        "Think exposing pandas/polars dataframe or duckdb with PostgreSQL wire protocol with pg_catalog compatibility.\""
    )

    assert (
        "readme = \"README.md\"" in project_section
    ), "[project] section must set readme to README.md for PyPI long description"

    assert (
        expected_description in project_section
    ), "[project] section must contain the expected one-line short description"

