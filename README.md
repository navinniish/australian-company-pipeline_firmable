# 🇦🇺 Australian Company Data Pipeline

A comprehensive ETL pipeline that **extracts**, **transforms**, and **loads** Australian company data from **Common Crawl** and the **Australian Business Register (ABR)** into a PostgreSQL database — featuring intelligent entity matching powered by **Large Language Models (LLMs)**.

---

## 🏗️ Architecture Overview

```text
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   Common Crawl  │    │       ABR       │    │    PostgreSQL    │
│   (~200k URLs)  │    │  (XML Extracts) │    │     Database     │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬────────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                             ETL PIPELINE                                 │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                    │
│  │  Extractors  │  │ Transformers │  │   Loaders    │                    │
│  │              │  │              │  │              │                    │
│  │ • CC Extract │  │ • LLM Match  │  │ • Core DB    │                    │
│  │ • ABR Parse  │  │ • Clean      │  │ • Analytics  │                    │
│  │ • Async I/O  │  │ • Normalize  │  │ • dbt Models │                    │
│  └──────────────┘  └──────────────┘  └──────────────┘                    │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                     LLM ENTITY MATCHING                            │  │
│  │  • OpenAI GPT-4 / Anthropic                                        │  │
│  │  • Semantic Similarity + Rule-Based Logic                          │  │
│  │  • Smart Prompts for Complex Cases                                 │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                              │
│                                                                          │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────────────────┐           │
│ │   Staging    │ │     Core     │ │       Analytics          │           │
│ │              │ │              │ │                          │           │
│ │ • raw_cc     │ │ • companies  │ │ • summary_stats          │           │
│ │ • raw_abr    │ │ • contacts   │ │ • industry_dist          │           │
│ │ • matches    │ │ • names      │ │ • geographic_analysis    │           │
│ └──────────────┘ └──────────────┘ └──────────────────────────┘           │
└──────────────────────────────────────────────────────────────────────────┘
