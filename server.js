#!/usr/bin/env node

/**
 * Forensic Risk Intelligence — MCP Server
 * Remote HTTP server for the portfolio chat widget.
 */

const http = require("http");
const https = require("https");

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";

const PROJECT_CONTEXT = `
You are an expert AI assistant embedded in Hemangi Tandle's data analytics portfolio.
You have deep knowledge of the "Forensic Risk Intelligence: Optimizing Audit Performance and Capital Protection" project.

PROJECT SUMMARY:
- Scale: 1,000,000+ forensic audit records
- Goal: Transition from reactive manual auditing to preemptive "Velocity-Gating"
- Capital at risk: $1.20M identified for protection
- Key finding: 68% of fraud incidents occur within the first 10 days (Primary Attack Window)

DATASETS:
1. Big 4 Financial Risk Insights (Kaggle, 2020-2025) — operational benchmarking across Deloitte, PwC, EY, KPMG
2. Bank Account Fraud Suite (Feedzai GitHub) — behavioral fraud pattern extraction and modeling

PIPELINE:
1. Alteryx — ETL: multi-source ingestion of 1M+ rows, complex joins, null remediation, temporal Maturity flag feature engineering
2. SQL — Window functions and CTEs to calculate Time-to-Risk (TTR), risk segmentation by velocity and impact
3. Excel — Risk-Return Frontier model, sensitivity analysis, $1,192,800 capital protection calculation
4. Tableau — 4-Chapter executive story: Risk Velocity, Operational Efficiency, Resource Optimization, Capital Protection

KEY FINDINGS:
- 1.0-Day Gap: Average TTR of 1.0 day proves manual audits are too slow to catch fraud
- 10-Day Threshold: 68% of incidents occur in first 10 days — optimal automated intervention point
- Efficiency Paradox: high workload firms like Deloitte have the highest risk exposure

LINKS:
- Tableau Story: https://public.tableau.com/app/profile/hemangi7471/viz/ForensicRiskIntelligenceOptimizingAuditPerformanceAndCapitalProtection/Story1
- GitHub Repo: https://github.com/Hemangit22/Forensic-Risk-Intelligence-Dashboard
- Large files (Google Drive): https://drive.google.com/drive/folders/1d_VfrOPAdfh32iHmGBcjiKuwK1x0yjhq

Answer questions knowledgeably, enthusiastically, and concisely. Direct visitors to the Tableau or GitHub links when relevant.
`;

// ── HELPERS ───────────────────────────────────────────────────────────────────

function setCORS(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

function readBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      try { resolve(JSON.parse(body)); }
      catch { resolve({}); }
    });
  });
}

async function callClaude(messages) {
  if (!ANTHROPIC_API_KEY) {
    return { ok: false, reply: "API key not configured on server." };
  }

  const body = JSON.stringify({
    model: "claude-haiku-4-5",
    max_tokens: 1024,
    system: PROJECT_CONTEXT,
    messages: messages,
  });

  console.log("Calling Claude with", messages.length, "message(s)...");
  console.log("Request body length:", body.length);

  return new Promise((resolve) => {
    const options = {
      hostname: "api.anthropic.com",
      path: "/v1/messages",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Length": Buffer.byteLength(body),
      },
    };

    const req = https.request(options, (res) => {
      let data = "";
      console.log("Claude API status:", res.statusCode);
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        console.log("Claude raw response:", data.substring(0, 300));
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            console.error("Claude API error:", parsed.error);
            resolve({ ok: false, reply: "Claude API error: " + parsed.error.message });
            return;
          }
          if (parsed.content && parsed.content.length > 0) {
            const textBlock = parsed.content.find(b => b.type === "text");
            if (textBlock) {
              resolve({ ok: true, reply: textBlock.text });
            } else {
              console.error("No text block found in content:", JSON.stringify(parsed.content));
              resolve({ ok: false, reply: "No text in Claude response." });
            }
          } else {
            console.error("Unexpected response shape:", JSON.stringify(parsed).substring(0, 200));
            resolve({ ok: false, reply: "Unexpected response from Claude." });
          }
        } catch (e) {
          console.error("JSON parse error:", e.message, "Raw:", data.substring(0, 200));
          resolve({ ok: false, reply: "Failed to parse Claude response." });
        }
      });
    });

    req.on("error", (e) => {
      console.error("Network error calling Claude:", e.message);
      resolve({ ok: false, reply: "Network error: " + e.message });
    });

    req.write(body);
    req.end();
  });
}

// ── ROUTE HANDLERS ────────────────────────────────────────────────────────────

async function handleChat(req, res) {
  console.log("POST /chat received");
  const body = await readBody(req);
  console.log("Body keys:", Object.keys(body));

  let messages = body.messages || [];

  if (!messages.length) {
    console.log("No messages in body");
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "No messages provided" }));
    return;
  }

  console.log("Last user message:", messages[messages.length - 1]?.content?.substring(0, 100));

  const result = await callClaude(messages);
  console.log("Claude result ok:", result.ok, "reply length:", result.reply.length);

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ reply: result.reply }));
}

function handleHealth(req, res) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "ok",
    server: "Forensic Risk Intelligence MCP Server",
    version: "2.0.0",
    api_configured: !!ANTHROPIC_API_KEY,
    model: "claude-haiku-4-5",
  }));
}

// ── MAIN SERVER ───────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  setCORS(res);

  const pathname = new URL(req.url, "http://localhost").pathname;
  console.log(req.method, pathname);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (pathname === "/" || pathname === "/health") {
    handleHealth(req, res);
  } else if (pathname === "/chat" && req.method === "POST") {
    await handleChat(req, res);
  } else {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Route not found: " + pathname }));
  }
});

server.listen(PORT, () => {
  console.log("=== Forensic Risk MCP Server started on port", PORT, "===");
  console.log("API key configured:", !!ANTHROPIC_API_KEY ? "YES" : "NO");
});
