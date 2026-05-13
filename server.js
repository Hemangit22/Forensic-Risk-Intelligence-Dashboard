#!/usr/bin/env node

/**
 * Forensic Risk Intelligence — MCP Server
 * Remote HTTP+SSE server for the portfolio chat widget.
 * Reads project files from GitHub and answers questions via Claude.
 */

const http = require("http");
const https = require("https");
const url = require("url");

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";

// GitHub raw base URL
const GITHUB_RAW = "https://raw.githubusercontent.com/Hemangit22/Forensic-Risk-Intelligence-Dashboard/main";

// Known files in the repo
const REPO_FILES = [
  { path: "README.md", label: "Project README & Overview" },
  { path: "01_Documentation", label: "Documentation folder" },
  { path: "04_SQL_Script", label: "SQL Scripts" },
  { path: "05_Excel_Analysis", label: "Excel Analysis" },
  { path: "06_Tableau_Workbooks", label: "Tableau Workbooks" },
];

const PROJECT_CONTEXT = `
You are an expert AI assistant embedded in Hemangi Tandle's data analytics portfolio.
You have deep knowledge of the "Forensic Risk Intelligence: Optimizing Audit Performance and Capital Protection" project.

PROJECT SUMMARY:
- Scale: 1,000,000+ forensic audit records
- Goal: Transition from reactive manual auditing to preemptive "Velocity-Gating"
- Capital at risk: $1.20M identified for protection
- Key finding: 68% of fraud incidents occur within the first 10 days (Primary Attack Window)

DATASETS:
1. Big 4 Financial Risk Insights (Kaggle, 2020-2025) — operational benchmarking
2. Bank Account Fraud Suite (Feedzai GitHub) — behavioral fraud pattern extraction

PIPELINE:
1. Alteryx — ETL: multi-source ingestion, cleansing, feature engineering (Maturity flags)
2. SQL — Window functions & CTEs to calculate Time-to-Risk (TTR), risk segmentation by velocity/impact
3. Excel — Risk-Return Frontier model, sensitivity analysis, $1,192,800 capital protection calculation
4. Tableau — 4-Chapter executive story: Risk Velocity, Operational Efficiency, Resource Optimization, Capital Protection

KEY FINDINGS:
- 1.0-Day Gap: Average TTR of 1.0 day proves manual audits are too slow
- 10-Day Threshold: optimal automated intervention point
- Efficiency Paradox: high workload firms (e.g. Deloitte) have highest risk exposure

LINKS:
- Tableau: https://public.tableau.com/app/profile/hemangi7471/viz/ForensicRiskIntelligenceOptimizingAuditPerformanceAndCapitalProtection/Story1
- GitHub: https://github.com/Hemangit22/Forensic-Risk-Intelligence-Dashboard
- Large files: https://drive.google.com/drive/folders/1d_VfrOPAdfh32iHmGBcjiKuwK1x0yjhq

Answer questions knowledgeably, enthusiastically, and concisely. Direct visitors to the Tableau or GitHub links when relevant.
`;

// ── HELPERS ──────────────────────────────────────────────────────────────────

function fetchGitHubFile(filePath) {
  return new Promise((resolve) => {
    const fileUrl = GITHUB_RAW + "/" + filePath;
    https.get(fileUrl, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve(res.statusCode === 200 ? data : null));
    }).on("error", () => resolve(null));
  });
}

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

async function callClaude(messages, systemPrompt) {
  if (!ANTHROPIC_API_KEY) {
    return "AI responses require an Anthropic API key. Please configure ANTHROPIC_API_KEY in the server environment.";
  }
  return new Promise((resolve) => {
    const body = JSON.stringify({
      model: "claude-sonnet-4-5",
      max_tokens: 1000,
      system: systemPrompt || PROJECT_CONTEXT,
      messages,
    });
    const req = https.request({
      hostname: "api.anthropic.com",
      path: "/v1/messages",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Length": Buffer.byteLength(body),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          const parsed = JSON.parse(data);
          resolve(parsed.content?.[0]?.text || "No response from Claude.");
        } catch {
          resolve("Error parsing Claude response.");
        }
      });
    });
    req.on("error", () => resolve("Network error calling Claude API."));
    req.write(body);
    req.end();
  });
}

// ── ROUTE HANDLERS ────────────────────────────────────────────────────────────

async function handleChat(req, res) {
  const body = await readBody(req);
  const messages = body.messages || [];
  const question = body.question || "";

  if (!messages.length && question) {
    messages.push({ role: "user", content: question });
  }

  if (!messages.length) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "No messages provided" }));
    return;
  }

  // Optionally enrich with README content
  let systemPrompt = PROJECT_CONTEXT;
  const lastUserMsg = messages[messages.length - 1]?.content?.toLowerCase() || "";
  if (lastUserMsg.includes("sql") || lastUserMsg.includes("script") || lastUserMsg.includes("query")) {
    const sqlContent = await fetchGitHubFile("04_SQL_Script/forensic_risk_queries.sql");
    if (sqlContent) {
      systemPrompt += "\n\nSQL SCRIPT CONTENT:\n" + sqlContent.substring(0, 3000);
    }
  }

  const reply = await callClaude(messages, systemPrompt);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ reply, model: "claude-sonnet-4-20250514" }));
}

async function handleFiles(req, res) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    files: REPO_FILES,
    github: "https://github.com/Hemangit22/Forensic-Risk-Intelligence-Dashboard",
    tableau: "https://public.tableau.com/app/profile/hemangi7471/viz/ForensicRiskIntelligenceOptimizingAuditPerformanceAndCapitalProtection/Story1",
  }));
}

async function handleReadFile(req, res, parsedUrl) {
  const filePath = parsedUrl.query.path;
  if (!filePath) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Missing ?path= parameter" }));
    return;
  }
  const content = await fetchGitHubFile(filePath);
  if (!content) {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "File not found on GitHub" }));
    return;
  }
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ path: filePath, content: content.substring(0, 10000) }));
}

function handleHealth(req, res) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "ok",
    server: "Forensic Risk Intelligence MCP Server",
    version: "1.0.0",
    api_configured: !!ANTHROPIC_API_KEY,
  }));
}

// ── MAIN SERVER ───────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  setCORS(res);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  if (pathname === "/health" || pathname === "/") {
    handleHealth(req, res);
  } else if (pathname === "/chat" && req.method === "POST") {
    await handleChat(req, res);
  } else if (pathname === "/files" && req.method === "GET") {
    await handleFiles(req, res);
  } else if (pathname === "/read" && req.method === "GET") {
    await handleReadFile(req, res, parsedUrl);
  } else {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Route not found" }));
  }
});

server.listen(PORT, () => {
  console.log("Forensic Risk Intelligence MCP Server running on port " + PORT);
  console.log("API key configured: " + (!!ANTHROPIC_API_KEY ? "YES" : "NO — set ANTHROPIC_API_KEY env var"));
});
