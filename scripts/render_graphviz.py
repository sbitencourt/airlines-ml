#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def ensure_graphviz_installed() -> None:
    """
    Ensure that Graphviz is installed and that the `dot` command
    is available in the system PATH.
    """
    try:
        subprocess.run(
            ["dot", "-V"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise SystemExit(
            "Graphviz is not installed or the `dot` command is not available in PATH.\n\n"
            "Installation instructions:\n"
            " - Windows (Chocolatey): choco install graphviz\n"
            " - macOS (Homebrew): brew install graphviz\n"
            " - Ubuntu/Debian: sudo apt-get install graphviz\n"
        ) from exc


def render_dot_to_pdf(dot_path: Path, output_dir: Path) -> Path:
    """
    Render a single .dot file to a PDF file using Graphviz.

    Parameters
    ----------
    dot_path : Path
        Path to the input .dot file.
    output_dir : Path
        Directory where the generated PDF will be saved.

    Returns
    -------
    Path
        Path to the generated PDF file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / f"{dot_path.stem}.pdf"

    # Command: dot -Tpdf input.dot -o output.pdf
    subprocess.run(
        ["dot", "-Tpdf", str(dot_path), "-o", str(pdf_path)],
        check=True,
    )

    return pdf_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render all Graphviz .dot files into PDF documents."
    )
    parser.add_argument(
        "--in-dir",
        default="docs/architecture",
        help="Directory where .dot files are located (default: docs/architecture)",
    )
    parser.add_argument(
        "--out-dir",
        default="docs/architecture/rendered",
        help="Output directory for generated PDF files "
             "(default: docs/architecture/rendered)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively search for .dot files inside the input directory",
    )

    args = parser.parse_args()
    input_dir = Path(args.in_dir)
    output_dir = Path(args.out_dir)

    if not input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {input_dir}")

    ensure_graphviz_installed()

    pattern = "**/*.dot" if args.recursive else "*.dot"
    dot_files = sorted(input_dir.glob(pattern))

    if not dot_files:
        print(f"No .dot files found in: {input_dir} (pattern: {pattern})")
        return

    print(
        f"Found {len(dot_files)} .dot file(s) in {input_dir}. "
        f"Generating PDFs in {output_dir}"
    )

    for dot_file in dot_files:
        try:
            pdf_path = render_dot_to_pdf(dot_file, output_dir)
            print(f"OK   {dot_file}  ->  {pdf_path}")
        except subprocess.CalledProcessError as exc:
            print(f"ERR  {dot_file} (Graphviz failed): {exc}")

    print("Done.")


if __name__ == "__main__":
    main()
