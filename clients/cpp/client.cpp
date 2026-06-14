// C++ client for arrow-parquet-dataserver.
//
// Demonstrates both transports:
//   * Option B — Arrow IPC over HTTP   (libcurl fetch + arrow::ipc::RecordBatchStreamReader)
//   * Option A — Arrow Flight (gRPC)   (arrow::flight::FlightClient)
//
// Arrow C++ transparently decompresses LZ4/ZSTD IPC buffers (the brew `apache-arrow`
// bottle is built with the codecs), so no extra setup is needed on the read path.
//
// Build:  see clients/cpp/CMakeLists.txt
// Run:    ./arrow_client --transport both --dataset optionmetrics --underlying AAPL --year 2023

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>

#include <curl/curl.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/flight/client.h>

namespace flight = arrow::flight;

// --------------------------------------------------------------------------- //
// Minimal JSON request builder (no external JSON dependency needed for output).
// --------------------------------------------------------------------------- //
struct Query {
  std::string dataset = "optionmetrics";
  std::string underlying = "AAPL";   // empty => omit
  std::string year = "2023";          // empty => omit
  std::string cp;                     // empty => omit
  std::string columns;                // comma-separated; empty => all
  std::string limit;                  // empty => no limit
};

static std::string BuildRequestJson(const Query& q) {
  std::ostringstream os;
  os << "{\"dataset\":\"" << q.dataset << "\"";

  if (!q.columns.empty()) {
    os << ",\"columns\":[";
    std::stringstream ss(q.columns);
    std::string col;
    bool first = true;
    while (std::getline(ss, col, ',')) {
      if (!first) os << ",";
      os << "\"" << col << "\"";
      first = false;
    }
    os << "]";
  }

  os << ",\"filters\":[";
  bool first = true;
  auto add_eq = [&](const std::string& c, const std::string& v, bool quoted) {
    if (!first) os << ",";
    os << "{\"column\":\"" << c << "\",\"op\":\"=\",\"value\":";
    if (quoted) os << "\"" << v << "\""; else os << v;
    os << "}";
    first = false;
  };
  if (!q.underlying.empty()) add_eq("underlying", q.underlying, true);
  if (!q.year.empty()) add_eq("year", q.year, false);
  if (!q.cp.empty()) add_eq("cp_flag", q.cp, true);
  os << "]";

  if (!q.limit.empty()) os << ",\"limit\":" << q.limit;
  os << "}";
  return os.str();
}

// --------------------------------------------------------------------------- //
// Option B: Arrow IPC over HTTP
// --------------------------------------------------------------------------- //
static size_t WriteCb(char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* out = static_cast<std::string*>(userdata);
  out->append(ptr, size * nmemb);
  return size * nmemb;
}

arrow::Result<std::shared_ptr<arrow::Table>> QueryHttp(
    const std::string& host, int port, const std::string& request_json,
    const std::string& compression, size_t* wire_bytes) {
  CURL* curl = curl_easy_init();
  if (!curl) return arrow::Status::IOError("curl init failed");

  std::string url = "http://" + host + ":" + std::to_string(port) +
                    "/query?format=arrow&compression=" + compression;
  std::string body;
  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_json.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);

  CURLcode rc = curl_easy_perform(curl);
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (rc != CURLE_OK)
    return arrow::Status::IOError("curl error: ", curl_easy_strerror(rc));
  if (http_code != 200)
    return arrow::Status::IOError("HTTP ", http_code, ": ", body.substr(0, 200));

  *wire_bytes = body.size();
  auto buffer = arrow::Buffer::FromString(std::move(body));
  auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(input));
  return reader->ToTable();
}

// --------------------------------------------------------------------------- //
// Option A: Arrow Flight (gRPC)
// --------------------------------------------------------------------------- //
arrow::Result<std::shared_ptr<arrow::Table>> QueryFlight(
    const std::string& host, int port, const std::string& request_json) {
  ARROW_ASSIGN_OR_RAISE(
      auto location, flight::Location::Parse("grpc://" + host + ":" + std::to_string(port)));
  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location));

  auto descriptor = flight::FlightDescriptor::Command(request_json);
  ARROW_ASSIGN_OR_RAISE(auto info, client->GetFlightInfo(descriptor));
  const auto& endpoints = info->endpoints();
  if (endpoints.empty()) return arrow::Status::Invalid("no flight endpoints");
  ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(endpoints[0].ticket));
  return stream->ToTable();
}

// --------------------------------------------------------------------------- //
static void Report(const std::string& label, const std::shared_ptr<arrow::Table>& t,
                   double ms, long wire_bytes) {
  std::cout << "[" << label << "] rows=" << t->num_rows()
            << "  cols=" << t->num_columns()
            << "  wall=" << ms << " ms";
  if (wire_bytes >= 0)
    std::cout << "  wire=" << (wire_bytes / 1e6) << " MB";
  std::cout << "\n          columns:";
  for (const auto& f : t->schema()->fields()) std::cout << " " << f->name();
  std::cout << "\n\n";
}

arrow::Status Run(int argc, char** argv) {
  std::map<std::string, std::string> args;
  for (int i = 1; i + 1 < argc; i += 2) args[argv[i]] = argv[i + 1];
  auto get = [&](const std::string& k, const std::string& d) {
    auto it = args.find(k);
    return it == args.end() ? d : it->second;
  };

  std::string transport = get("--transport", "both");
  std::string host = get("--host", "127.0.0.1");
  int http_port = std::stoi(get("--http-port", "8000"));
  int flight_port = std::stoi(get("--flight-port", "8815"));
  std::string compression = get("--compression", "zstd");

  Query q;
  q.dataset = get("--dataset", "optionmetrics");
  q.underlying = get("--underlying", "AAPL");
  if (q.underlying == "none") q.underlying.clear();
  q.year = get("--year", "2023");
  if (q.year == "none") q.year.clear();
  q.cp = get("--cp", "");
  q.columns = get("--columns", "");
  q.limit = get("--limit", "");

  std::string request_json = BuildRequestJson(q);
  std::cout << "Request: " << request_json << "\n\n";

  using clock = std::chrono::steady_clock;
  if (transport == "http" || transport == "both") {
    size_t wire = 0;
    auto t0 = clock::now();
    ARROW_ASSIGN_OR_RAISE(auto table, QueryHttp(host, http_port, request_json, compression, &wire));
    double ms = std::chrono::duration<double, std::milli>(clock::now() - t0).count();
    Report("HTTP  (arrow-ipc)", table, ms, static_cast<long>(wire));
  }
  if (transport == "flight" || transport == "both") {
    auto t0 = clock::now();
    ARROW_ASSIGN_OR_RAISE(auto table, QueryFlight(host, flight_port, request_json));
    double ms = std::chrono::duration<double, std::milli>(clock::now() - t0).count();
    Report("Flight (gRPC)", table, ms, -1);
  }
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  arrow::Status st = Run(argc, argv);
  curl_global_cleanup();
  if (!st.ok()) {
    std::cerr << "ERROR: " << st.ToString() << "\n";
    return 1;
  }
  return 0;
}
