#!/usr/bin/env node

/**
 * Forensic Risk Intelligence — MCP Server
 * Uses Google Gemini API (free tier) instead of Anthropic.
 */

const http = require("http");
const https = require("https");

const PORT = process.env.PORT || 10000;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";

const PROJECT_CONTEXT = `You are an expert AI assistant embedded in Hemangi Tandle's data analytics portfolio.
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

Answer questions knowledgeably, enthusiastically, and concisely. Direct visitors to the Tableau or GitHub links when relevant. Keep answers under 150 words.`;

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

async function callGemini(messages) {
  if (!GEMINI_API_KEY) {
    return { ok: false, reply: "Gemini API key not configured on server." };
  }

  // Convert messages array to Gemini format
  // Prepend project context to the first user message
  const geminiContents = messages.map((msg, index) => {
    let text = msg.content;
    if (index === 0 && msg.role === "user") {
      text = PROJECT_CONTEXT + "\n\nVisitor question: " + msg.content;
    }
    return {
      role: msg.role === "assistant" ? "model" : "user",
      parts: [{ text }],
    };
  });

  const geminiBody = JSON.stringify({
    contents: geminiContents,
    generationConfig: {
      maxOutputTokens: 512,
      temperature: 0.7,
    },
  });

  console.log("Calling Gemini with", messages.length, "message(s)...");

  return new Promise((resolve) => {
    const options = {
      hostname: "generativelanguage.googleapis.com",
      path: `/v1beta/models/gemini-1.5-flash:generateContent?key=${GEMINI_API_KEY}`,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(geminiBody),
      },
    };

    const req = https.request(options, (res) => {
      let data = "";
      console.log("Gemini API status:", res.statusCode);
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        console.log("Gemini raw response:", data.substring(0, 300));
        try {
          const parsed = JSON.parse(data);

          // Check for API errors
          if (parsed.error) {
            console.error("Gemini API error:", parsed.error.message);
            resolve({ ok: false, reply: "Gemini API error: " + parsed.error.message });
            return;
          }

          // Extract text from Gemini response structure
          const text = parsed?.candidates?.[0]?.content?.parts?.[0]?.text;
          if (text) {
            resolve({ ok: true, reply: text });
          } else {
            console.error("No text in Gemini response:", JSON.stringify(parsed).substring(0, 300));
            resolve({ ok: false, reply: "No response text from Gemini." });
          }
        } catch (e) {
          console.error("Parse error:", e.message);
          resolve({ ok: false, reply: "Failed to parse Gemini response." });
        }
      });
    });

    req.on("error", (e) => {
      console.error("Network error:", e.message);
      resolve({ ok: false, reply: "Network error: " + e.message });
    });

    req.write(geminiBody);
    req.end();
  });
}

// ── ROUTE HANDLERS ────────────────────────────────────────────────────────────

async function handleChat(req, res) {
  console.log("POST /chat received");
  const body = await readBody(req);

  let messages = body.messages || [];
  if (!messages.length) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "No messages provided" }));
    return;
  }

  console.log("Last user message:", messages[messages.length - 1]?.content?.substring(0, 100));

  const result = await callGemini(messages);
  console.log("Gemini result ok:", result.ok, "| reply length:", result.reply.length);

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ reply: result.reply }));
}

function handleHealth(req, res) {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "ok",
    server: "Forensic Risk Intelligence MCP Server",
    version: "3.0.0",
    api: "Google Gemini (free tier)",
    api_configured: !!GEMINI_API_KEY,
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
  console.log("API: Google Gemini | Key configured:", !!GEMINI_API_KEY ? "YES" : "NO");
});
