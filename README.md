```mermaid
flowchart TD

  %% Main wrapper (vertical)
  subgraph "Algo Trading System"
    direction TB

    %% --- LIVE SESSION ---
    subgraph LIVE["Live Session <br/>(09:15–15:15 IST)"]
      TRADING_PLATFORM
      ZW["Zerodha WebSocket<br/>(ticks)"]:::source -->|"validate (JSON Schema)"| T[(Kafka: ticks)]:::kafka
      T --> B1S["Bar Builder 1s<br/>(ticks→1s bars)"]:::process
      B1S --> K1S[(Kafka: bars.1s)]:::kafka
      K1S --> AGG["Generic Aggregator<br/>(1s→1m/3m/5m/15m…)"]:::process
      AGG -->|1m persisted| DB1[(Postgres: bars_1m)]:::db
      AGG -->|3m/5m/15m…| KTF[(Kafka: bars.tf.*)]:::kafka

      DB1 --> STR["Strategy Runner<br/>(BaseStrategy children)"]:::strategy
      KTF --> STR
      STR --> RISK["Risk & Compliance<br/>(broker budget, caps, DD,<br/>lot/tick/price-band checks,<br/>throttles)"]:::risk
      RISK --> OMS["OMS/EMS<br/>(idempotency, retries/backoff, routing)"]:::oms

      OMS -->|paper| PG["Paper Gateway"]:::gateway
      OMS -->|matcher| PM["Paper Gateway +<br/>C++ Matcher"]:::gateway
      PM --> CXX[["C++ Price-Time Matcher"]]:::engine
      OMS -->|live/shadow| LZ["Live Zerodha Gateway<br/>(place/modify/cancel/poll)"]:::gateway

      CXX --> FILLS[(Kafka: fills)]:::kafka
      PG  --> FILLS
      LZ  --> FILLS

      style LIVE fill:#E0EAF4,stroke:#A3B7C9,stroke-width:3px,color:#000000;
    end

    %% --- AFTER CLOSE ---
    subgraph EOD["After Close"]
      direction TB
      DB1 -->|compare| VND["Zerodha 1m<br/>(historical)"]:::source
      VND --> RECON["(bars_1m_recon)"]:::process
      RECON --> GVIEW["VIEW bars_1m_golden<br/>(recon ∪ raw)"]:::db
      GVIEW --> S3["(S3 Parquet Archive<br/>>100 days)"]:::storage
      GVIEW --> IND["Indicators (15–20)"]:::process
      IND --> ENS["Ensemble Signals"]:::output
      GVIEW --> BT["Backtests & Param Sweeps"]:::process
      BT --> TOP5["Top-5 Configs<br/>(next day candidates)"]:::output
      GVIEW --> PAIR["Pairs Scan<br/>(corr/cointegration)"]:::process
      PAIR --> WATCH["Next-Day Watchlist"]:::output

      style EOD fill:#EAF4EA,stroke:#A3C9A3,stroke-width:3px,color:#000000;
    end

    %% --- OBSERVABILITY ---
    subgraph OBS["Observability & Ops"]
      direction TB
      PRM["Prometheus"]:::monitoring --> GRA["Grafana Dashboards & Alerts"]:::monitoring
      DOC["Doctor/One-click<br/>(start/stop/health)"]:::controls

      style OBS fill:#F4ECE0,stroke:#C9B7A3,stroke-width:3px,color:#000000;
    end

    %% --- Vertical stacking helpers (layout-only, dashed) ---
    LIVE -.-> EOD
    %% OBS  -.-> EOD

    %% --- Metrics & controls ---
    B1S --metrics--> PRM
    STR --metrics--> PRM
    RISK --metrics--> PRM
    OMS --metrics--> PRM
    PM --metrics--> PRM
    LZ --metrics--> PRM
    DOC --controls--> LIVE
    DOC --controls--> EOD
  end

  %% --- Styles (kept simple for GitHub)
  classDef source fill:#B3D9FF,stroke:#265A8F,stroke-width:3px,color:#000000;
  classDef kafka fill:#FFC48C,stroke:#B26200,stroke-width:3px,color:#000000;
  classDef process fill:#A3D7AE,stroke:#1A692D,stroke-width:3px,color:#000000;
  classDef db fill:#8CB3FF,stroke:#0047A3,stroke-width:3px,color:#000000;
  classDef strategy fill:#FFDD99,stroke:#B27C00,stroke-width:3px,color:#000000;
  classDef risk fill:#E5A9AC,stroke:#8A1C25,stroke-width:3px,color:#000000;
  classDef oms fill:#C4A2C9,stroke:#664B86,stroke-width:3px,color:#000000;
  classDef gateway fill:#99C2FF,stroke:#0047A3,stroke-width:3px,color:#000000;
  classDef engine fill:#B2D96B,stroke:#5A7A25,stroke-width:3px,color:#000000;
  classDef storage fill:#A3C9D6,stroke:#1A5A69,stroke-width:3px,color:#000000;
  classDef output fill:#A3E0C9,stroke:#1A8050,stroke-width:3px,color:#000000;
  classDef monitoring fill:#FFCC80,stroke:#A36600,stroke-width:3px,color:#000000;
  classDef controls fill:#D9D9D9,stroke:#595959,stroke-width:3px,color:#000000;

  linkStyle default stroke-width:3px,fill:none,stroke:#555
