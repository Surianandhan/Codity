"""Markdown -> HTML. Mermaid fenced blocks are pulled out of the RAW markdown
source before conversion (codehilite restructures ALL fenced code blocks,
including mermaid ones, into a form with no language marker left to detect --
so detection has to happen before markdown.convert(), not after)."""
import re
import markdown
from pathlib import Path

SRC = Path("/Users/AIRUS/Documents/Codity/submission/combined.md")
OUT = Path("/Users/AIRUS/Documents/Codity/submission/combined.html")

md_text = SRC.read_text()

mermaid_blocks = []

def extract_mermaid(m: re.Match) -> str:
    mermaid_blocks.append(m.group(1))
    return f"\n\n@@MERMAID_PLACEHOLDER_{len(mermaid_blocks) - 1}@@\n\n"

md_text = re.sub(r"```mermaid\n(.*?)```", extract_mermaid, md_text, flags=re.S)
print(f"extracted {len(mermaid_blocks)} mermaid diagrams")

md = markdown.Markdown(
    extensions=["extra", "fenced_code", "tables", "toc", "codehilite", "sane_lists"],
    extension_configs={
        "codehilite": {"guess_lang": False, "css_class": "hl"},
        "toc": {"permalink": False, "toc_depth": "2-3"},
    },
)
body_html = md.convert(md_text)

for i, raw in enumerate(mermaid_blocks):
    placeholder_p = f"<p>@@MERMAID_PLACEHOLDER_{i}@@</p>"
    replacement = f'<pre class="mermaid">{raw}</pre>'
    if placeholder_p not in body_html:
        raise RuntimeError(f"placeholder {i} not found verbatim in HTML output")
    body_html = body_html.replace(placeholder_p, replacement)

pygments_css = ""
try:
    from pygments.formatters import HtmlFormatter
    pygments_css = HtmlFormatter(style="friendly").get_style_defs(".hl")
except Exception as e:
    print("pygments css skipped:", e)

CSS = f"""
@page {{
  size: A4;
  margin: 20mm 16mm 22mm 16mm;
  @bottom-center {{
    content: "Codity — Distributed Job Scheduler   |   page " counter(page) " of " counter(pages);
    font-size: 8pt;
    color: #888;
  }}
}}
* {{ box-sizing: border-box; }}
:root {{ color-scheme: light only; }}
body {{
  font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
  font-size: 10.3pt;
  line-height: 1.5;
  color: #1a1a1a;
  max-width: 100%;
}}
h1 {{
  font-size: 22pt;
  border-bottom: 3px solid #2b6cb0;
  padding-bottom: 6px;
  margin-top: 0;
  color: #1a365d;
}}
h2 {{
  font-size: 15pt;
  color: #1a365d;
  border-bottom: 1px solid #cbd5e0;
  padding-bottom: 3px;
  margin-top: 26px;
}}
h3 {{ font-size: 12.5pt; color: #2c5282; margin-top: 18px; }}
h4 {{ font-size: 11pt; color: #2d3748; }}
h1, h2, h3 {{ page-break-after: avoid; }}
p, ul, ol, table, pre {{ page-break-inside: avoid; }}
code {{
  background: #f1f3f5;
  padding: 1px 4px;
  border-radius: 3px;
  font-family: "SF Mono", Menlo, Consolas, monospace;
  font-size: 8.7pt;
}}
pre {{
  background: #f6f8fa;
  border: 1px solid #e2e8f0;
  border-radius: 5px;
  padding: 10px 12px;
  overflow-x: auto;
  font-size: 8.2pt;
  line-height: 1.4;
}}
pre code {{ background: none; padding: 0; font-size: inherit; }}
div.hl {{ page-break-inside: avoid; }}
pre.mermaid {{
  background: white;
  border: 1px solid #e2e8f0;
  text-align: center;
  padding: 14px;
  page-break-inside: avoid;
}}
pre.mermaid svg {{ max-width: 100%; height: auto; }}
table {{
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0;
  font-size: 9pt;
}}
th, td {{
  border: 1px solid #cbd5e0;
  padding: 5px 8px;
  text-align: left;
  vertical-align: top;
}}
th {{ background: #edf2f7; font-weight: 600; }}
tr:nth-child(even) td {{ background: #fafbfc; }}
blockquote {{
  border-left: 4px solid #2b6cb0;
  margin: 10px 0;
  padding: 4px 14px;
  color: #4a5568;
  background: #f7fafc;
}}
a {{ color: #2b6cb0; text-decoration: none; }}
hr {{ border: none; border-top: 1px solid #cbd5e0; margin: 20px 0; }}
.pagebreak {{ page-break-before: always; }}
.toc {{ background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 14px 20px; }}
.toc ul {{ list-style: none; padding-left: 16px; }}
.toc > ul {{ padding-left: 0; }}
.toc a {{ color: #2d3748; }}
{pygments_css}
"""

HTML = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="color-scheme" content="light only">
<title>Codity — Assignment Submission</title>
<style>{CSS}</style>
</head>
<body>
{body_html}
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>
  mermaid.initialize({{ startOnLoad: false, theme: "default", securityLevel: "loose" }});
  window.mermaidDone = false;
  mermaid.run({{ querySelector: ".mermaid" }}).then(() => {{ window.mermaidDone = true; }})
    .catch((e) => {{ console.error("mermaid error", e); window.mermaidDone = true; }});
</script>
</body>
</html>
"""

OUT.write_text(HTML)
print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
