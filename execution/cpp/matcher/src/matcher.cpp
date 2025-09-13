// execution/cpp/matcher/src/matcher.cpp
// Minimal JSON-lines TCP server (port 5555).
// - Supports ops: ping, exec
// - For exec, creates 1-3 partial fills around ref_price with small spread.
// - Threaded accept loop; single-threaded handling for simplicity.
// Build: via CMakeLists in this folder.
// NOTE: This is a didactic matching engine, not production-grade.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cmath>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <random>
#include <map>

// Very tiny JSON encoder for responses (to avoid heavy deps). Inputs trusted from Python.
static std::string json_escape(const std::string& s) {
    std::string out; out.reserve(s.size()+2);
    for (char c : s) {
        if (c=='"' || c=='\\') { out.push_back('\\'); out.push_back(c); }
        else if (c=='\n') { out += "\\n"; }
        else out.push_back(c);
    }
    return out;
}

static std::string fills_response(const std::vector<std::pair<int,double>>& fills) {
    std::ostringstream oss;
    oss << "{\"ok\":true,\"fills\":[";
    for (size_t i=0;i<fills.size();++i) {
        if (i) oss << ',';
        oss << "{\"qty\":" << fills[i].first << ",\"price\":" << std::fixed << fills[i].second << "}";
    }
    oss << "]}";
    return oss.str();
}

static std::string error_response(const std::string& msg) {
    std::ostringstream oss;
    oss << "{\"ok\":false,\"err\":\"" << json_escape(msg) << "\"}";
    return oss.str();
}

static bool parse_field(const std::string& body, const std::string& key, std::string& out) {
    auto pos = body.find("\""+key+"\"");
    if (pos==std::string::npos) return false;
    pos = body.find(':', pos); if (pos==std::string::npos) return false;
    // crude parse: handles numbers and quoted strings
    size_t i = pos+1;
    while (i<body.size() && isspace(body[i])) ++i;
    if (i>=body.size()) return false;
    if (body[i]=='"') { // string
        size_t j = body.find('"', i+1);
        if (j==std::string::npos) return false;
        out = body.substr(i+1, j-(i+1));
        return true;
    } else { // number / literal
        size_t j=i;
        while (j<body.size() && std::string(",} \n\r\t").find(body[j])==std::string::npos) ++j;
        out = body.substr(i, j-i);
        return true;
    }
}

int main(int argc, char** argv) {
    int port = 5555;
    if (const char* p = std::getenv("MATCHER_PORT")) port = std::atoi(p);

    int serv_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (serv_fd < 0) { perror("socket"); return 1; }

    int yes=1; setsockopt(serv_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(serv_fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(serv_fd, 16) < 0) { perror("listen"); return 1; }

    std::cout << "[matcher] listening on " << port << std::endl;

    std::random_device rd; std::mt19937 gen(rd());

    while (true) {
        sockaddr_in cli{}; socklen_t clen=sizeof(cli);
        int cfd = accept(serv_fd, (sockaddr*)&cli, &clen);
        if (cfd < 0) { perror("accept"); continue; }

        // Read one line (JSON) then respond and close (simple request/response).
        std::string buf; buf.reserve(1024);
        char tmp[512]; ssize_t n;
        while ((n = read(cfd, tmp, sizeof(tmp))) > 0) {
            buf.append(tmp, tmp+n);
            if (!buf.empty() && buf.back()=='\n') break;
            if (buf.size() > 65536) break; // guard
        }

        std::string op;
        if (!parse_field(buf, "op", op)) {
            auto resp = error_response("missing_op") + "\n";
            write(cfd, resp.data(), resp.size()); close(cfd); continue;
        }

        if (op=="ping") {
            std::string resp = "{\"ok\":true}\n";
            write(cfd, resp.data(), resp.size()); close(cfd); continue;
        }

        if (op=="exec") {
            std::string ssym, sside, sqty, sref;
            if (!parse_field(buf, "symbol", ssym) || !parse_field(buf, "side", sside) || !parse_field(buf, "qty", sqty)) {
                auto resp = error_response("bad_args") + "\n";
                write(cfd, resp.data(), resp.size()); close(cfd); continue;
            }
            int qty = std::max(1, std::atoi(sqty.c_str()));
            double ref = 100.0; // default ref price
            if (parse_field(buf, "ref_price", sref)) {
                try { ref = std::stod(sref); } catch(...) {}
            }

            // Generate 1-3 partial fills around ref price within a small micro-spread.
            std::uniform_int_distribution<int> parts(1,3);
            std::normal_distribution<double> tick(0.0, 0.02);  // ~2 paise std dev
            int nfill = parts(gen);
            std::vector<std::pair<int,double>> fills; fills.reserve(nfill);
            int remain=qty;

            for (int i=0;i<nfill;i++) {
                int q = (i==nfill-1) ? remain : std::max(1, remain / (nfill - i));
                remain -= q;
                double px = ref + tick(gen);
                // Slight bias: buy crosses up, sell crosses down.
                if (sside=="BUY")   px += 0.005;
                if (sside=="SELL")  px -= 0.005;
                px = std::max(0.01, px);
                fills.emplace_back(q, px);
            }

            std::string resp = fills_response(fills) + "\n";
            write(cfd, resp.data(), resp.size()); close(cfd); continue;
        }

        std::string resp = error_response("unknown_op") + "\n";
        write(cfd, resp.data(), resp.size()); close(cfd);
    }
    return 0;
}
