# Presentation

**`kob.pptx`** — a 9-slide deck presenting kob: motivation, a 30-second theory intro,
architecture, key findings, the performance comparison, and the demo.

| # | Slide |
|---|---|
| 1 | Title |
| 2 | Motivation — serving a folder of Parquet shouldn't be hard or slow |
| 3 | What kob is — tiny / self-configuring / fast / safe |
| 4 | Theory — why Apache Arrow (row vs columnar) |
| 5 | Architecture — Flight + Swagger, one command |
| 6 | Key findings — speedup chart |
| 7 | Performance — same query, five wire formats (latency chart) |
| 8 | Demo — one query, every transport |
| 9 | Takeaways |

## Rebuild

The deck is generated from `build_deck.py`, so it stays in sync with the numbers in
[`docs/PERFORMANCE_REPORT.md`](../docs/PERFORMANCE_REPORT.md):

```bash
uv run --with python-pptx --with matplotlib python presentation/build_deck.py
```

This regenerates the two charts in `assets/` and writes `kob.pptx`.

## View

Open `kob.pptx` in PowerPoint, Keynote, Google Slides, or LibreOffice Impress.
