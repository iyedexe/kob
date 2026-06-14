// Power Query (M) — the practical way to pull kob data into Excel on the desktop.
//
// Excel cannot parse Apache Arrow natively, so this POSTs the standard query contract
// to `/query?format=csv`. DuckDB still does all the filtering/pushdown server-side, so
// only the rows you asked for cross the wire.
//
// How to use:
//   Excel > Data > Get Data > From Other Sources > Blank Query > Advanced Editor,
//   then paste this. Adjust BaseUrl / Dataset / Columns / Filters / Limit.

let
    BaseUrl = "http://127.0.0.1:8000",
    Body = [
        dataset = "optionmetrics",
        columns = {"date", "strike", "impl_vol", "delta", "und_price"},
        filters = {
            [column = "underlying", op = "=", value = "AAPL"],
            [column = "year",       op = "=", value = 2023],
            [column = "cp_flag",    op = "=", value = "C"]
        },
        limit = 20000
    ],

    Source = Csv.Document(
        Web.Contents(
            BaseUrl,
            [
                RelativePath = "query",
                Query = [format = "csv"],
                Headers = [#"Content-Type" = "application/json"],
                Content = Json.FromValue(Body)
            ]
        ),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Promoted,
        {{"strike", type number}, {"impl_vol", type number},
         {"delta", type number}, {"und_price", type number}})
in
    Typed
