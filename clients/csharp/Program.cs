// C#/.NET client for arrow-parquet-dataserver.
//
// Demonstrates both transports:
//   * Option B — Arrow IPC over HTTP   (System.Net.Http + Apache.Arrow ArrowStreamReader)
//   * Option A — Arrow Flight (gRPC)   (Apache.Arrow.Flight FlightClient)
//
// Build & run:
//   dotnet run --project clients/csharp -- --transport flight --dataset optionmetrics --underlying AAPL --year 2023
//
// Key .NET gotchas handled here:
//   * LZ4/ZSTD IPC needs the separate Apache.Arrow.Compression package + a
//     CompressionCodecFactory passed to ArrowStreamReader.
//   * gRPC's default MaxReceiveMessageSize (4 MB) rejects large RecordBatches — raised below.
//   * pyarrow's Flight server speaks plaintext h2c (grpc://), so we enable unencrypted HTTP/2.

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Compression;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Ipc;
using Grpc.Core;
using Grpc.Net.Client;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var opt = Options.Parse(args);
        string requestJson = BuildRequestJson(opt);
        Console.WriteLine($"Request: {requestJson}\n");

        if (opt.Transport is "http" or "both")
            await RunHttp(opt, requestJson);
        if (opt.Transport is "flight" or "both")
            await RunFlight(opt, requestJson);

        return 0;
    }

    // ------------------------------------------------------------------ HTTP
    static async Task RunHttp(Options opt, string requestJson)
    {
        string url = $"http://{opt.Host}:{opt.HttpPort}/query?format=arrow&compression={opt.Compression}";
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(10) };

        var sw = Stopwatch.StartNew();
        using var content = new StringContent(requestJson, Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync(url, content);
        resp.EnsureSuccessStatusCode();
        await using var stream = await resp.Content.ReadAsStreamAsync();

        // CompressionCodecFactory enables transparent LZ4/ZSTD decompression.
        using var reader = new ArrowStreamReader(stream, new CompressionCodecFactory());

        long rows = 0;
        int batches = 0;
        Schema? schema = null;
        RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
        {
            schema ??= reader.Schema;
            rows += batch.Length;
            batches++;
            batch.Dispose();
        }
        sw.Stop();
        Report("HTTP  (arrow-ipc)", rows, batches, schema, sw.Elapsed);
    }

    // ----------------------------------------------------------------- Flight
    static async Task RunFlight(Options opt, string requestJson)
    {
        // pyarrow.flight serves plaintext h2c; allow unencrypted HTTP/2.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        using var channel = GrpcChannel.ForAddress(
            $"http://{opt.Host}:{opt.FlightPort}",
            new GrpcChannelOptions
            {
                // Default is 4 MB — far too small for bulk RecordBatches.
                MaxReceiveMessageSize = 1024 * 1024 * 1024,
                MaxSendMessageSize = 64 * 1024 * 1024,
            });

        var client = new FlightClient(channel);
        var descriptor = FlightDescriptor.CreateCommandDescriptor(requestJson);

        var sw = Stopwatch.StartNew();
        FlightInfo info = await client.GetInfo(descriptor);
        FlightEndpoint endpoint = info.Endpoints[0];

        var streamingCall = client.GetStream(endpoint.Ticket);
        long rows = 0;
        int batches = 0;
        await foreach (RecordBatch batch in streamingCall.ResponseStream.ReadAllAsync())
        {
            rows += batch.Length;
            batches++;
            batch.Dispose();
        }
        sw.Stop();
        Report("Flight (gRPC)", rows, batches, info.Schema, sw.Elapsed);
    }

    // ----------------------------------------------------------------- helpers
    static void Report(string label, long rows, int batches, Schema? schema, TimeSpan elapsed)
    {
        string cols = schema is null ? "?" : string.Join(", ", schema.FieldsList.Select(f => f.Name));
        double mrows = rows / elapsed.TotalSeconds / 1e6;
        Console.WriteLine($"[{label}] rows={rows:N0}  batches={batches}  wall={elapsed.TotalMilliseconds:F1} ms  ({mrows:F2} M rows/s)");
        Console.WriteLine($"          columns: {cols}\n");
    }

    static string BuildRequestJson(Options opt)
    {
        var filters = new List<object>();
        if (opt.Underlying is not null)
            filters.Add(new { column = "underlying", op = "=", value = opt.Underlying });
        if (opt.Year is not null)
            filters.Add(new { column = "year", op = "=", value = opt.Year });
        if (opt.Cp is not null)
            filters.Add(new { column = "cp_flag", op = "=", value = opt.Cp });

        var req = new Dictionary<string, object?>
        {
            ["dataset"] = opt.Dataset,
            ["columns"] = opt.Columns,
            ["filters"] = filters,
            ["limit"] = opt.Limit,
        };
        return JsonSerializer.Serialize(req);
    }
}

record Options
{
    public string Transport = "flight";
    public string Host = "127.0.0.1";
    public int HttpPort = 8001;
    public int FlightPort = 8815;
    public string Dataset = "optionmetrics";
    public string? Underlying = "AAPL";
    public int? Year = 2023;
    public string? Cp;
    public string[]? Columns;
    public int? Limit;
    public string Compression = "zstd";

    public static Options Parse(string[] args)
    {
        var o = new Options();
        for (int i = 0; i < args.Length - 1; i += 2)
        {
            string v = args[i + 1];
            switch (args[i])
            {
                case "--transport": o.Transport = v; break;
                case "--host": o.Host = v; break;
                case "--http-port": o.HttpPort = int.Parse(v); break;
                case "--flight-port": o.FlightPort = int.Parse(v); break;
                case "--dataset": o.Dataset = v; break;
                case "--underlying": o.Underlying = v == "none" ? null : v; break;
                case "--year": o.Year = v == "none" ? null : int.Parse(v); break;
                case "--cp": o.Cp = v; break;
                case "--columns": o.Columns = v.Split(','); break;
                case "--limit": o.Limit = int.Parse(v); break;
                case "--compression": o.Compression = v; break;
            }
        }
        return o;
    }
}
