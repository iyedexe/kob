/**
 * Office Script (Excel) — POST the kob query contract to /query?format=json and write
 * the rows into the active worksheet.
 *
 * Paste into Excel > Automate > New Script.
 *
 * IMPORTANT: Office Scripts run in a sandbox. `fetch` to `http://127.0.0.1` only works
 * if the runtime can actually reach that address (e.g. Excel desktop on the same machine
 * with external fetch enabled). For a server reachable on the network, use its hostname
 * and enable HTTPS. For purely local use, prefer Power Query (power_query.m) or xlwings.
 */
async function main(workbook: ExcelScript.Workbook) {
  const url = "http://127.0.0.1:8000/query?format=json";
  const body = {
    dataset: "optionmetrics",
    columns: ["date", "strike", "impl_vol", "delta", "und_price"],
    filters: [
      { column: "underlying", op: "=", value: "AAPL" },
      { column: "year", op: "=", value: 2023 },
      { column: "cp_flag", op: "=", value: "C" },
    ],
    limit: 5000,
  };

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`Server returned ${response.status}: ${await response.text()}`);
  }
  const rows = (await response.json()) as Record<string, string | number>[];
  if (rows.length === 0) {
    return;
  }

  const headers = Object.keys(rows[0]);
  const values: (string | number)[][] = [headers];
  for (const row of rows) {
    values.push(headers.map((h) => row[h]));
  }

  const sheet = workbook.getActiveWorksheet();
  sheet.getRangeByIndexes(0, 0, values.length, headers.length).setValues(values);
  sheet.getUsedRange().getFormat().autofitColumns();
}
