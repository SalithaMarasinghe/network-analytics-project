# **Dashboard 1 – Customer Health & Churn Intelligence**

![](C:\Users\Salitha\AppData\Roaming\marktext\images\2026-05-29-20-04-38-image.png)

## Purpose

Dashboard 1 is a **customer health and churn intelligence** view for an ISP (modelled on Superloop). It answers four strategic questions:

1. How many customers do we have, and how many are churning?

2. Which products, regions, and tenure segments are driving churn?

3. How does network/service quality (SLA breaches, tickets) relate to churn?

4. How can leaders slice this by segment (plan, region, time, tenure)?

The page uses your Databricks gold model (DimCustomer, DimRegion, DimDate, FactTelemetry, FactTickets, FactBilling) with star‑schema relationships to deliver interactive visuals.

---

## KPIs (Card visuals)

Across the top row, you placed **four KPI cards** (transparent background, white text) aligned to the Canva layout:

1. **Total Customers**
   
   - Measure:
     
     text
     
     `Total Customers = COUNTROWS(dimcustomer)`
   
   - Shows current customer base size.

2. **Churned Customers (Last 12M, or overall depending on filter)**
   
   - Measure (logic): count of rows in `DimCustomer` where `churn_flag = TRUE()`.
   
   - Indicates absolute churn volume.

3. **Churn Rate %**
   
   - Measure:
     
     text
     
     `Churned Customers = CALCULATE(     COUNTROWS(dimcustomer),    dimcustomer[churn_flag] = TRUE() ) Churn Rate % = DIVIDE(     [Churned Customers],    [Total Customers] ) * 100`
   
   - Central performance KPI for an ISP.

4. **Average Tenure (Months)**
   
   - Measure:
     
     text
     
     `Avg Tenure (Months) = AVERAGE(dimcustomer[tenure_months])`
   
   - Indicates how “sticky” the customer base is.

These KPIs respond to all page filters (date, region, plan, etc.), so leadership can see churn metrics for any segment.

---

## Visualizations

## 1) Churn Rate % by Plan Type (Hero visual)

- **Type:** Clustered bar chart.

- **Fields:**
  
  - Axis: `DimCustomer[plan_type]` (e.g., NBN50, NBN100, Fibre).
  
  - Values: `Churn Rate %` measure.

- **Use:** Shows which product plans have the highest churn, sorted descending so problem plans appear at the top. This reflects common telco churn analysis practices.

## 2) Churn by Region (City)

- **Type:** Column or bar chart.

- **Fields:**
  
  - Axis: `DimRegion[region]` (Sydney, Perth, Canberra, Brisbane, etc.).
  
  - Values: `Churn Rate %` or `Churned Customers`.

- **Use:** Identifies geographic hotspots where churn is higher, hinting at network or competitive issues in specific cities.

## 3) Churn by Tenure Bucket

- **Prep:** Calculated column in `DimCustomer`:
  
  text
  
  `Tenure Bucket = SWITCH(     TRUE(),    dimcustomer[tenure_months] <= 6, "0–6 months",    dimcustomer[tenure_months] <= 12, "7–12 months",    dimcustomer[tenure_months] <= 24, "13–24 months",    "24+ months" )`

- **Visual type:** Column or area chart.

- **Fields:**
  
  - Axis: `DimCustomer[Tenure Bucket]`.
  
  - Values: `Churn Rate %`.

- **Use:** Shows when in the lifecycle churn is highest (early onboarding vs long‑tenure churn), helping design retention strategies and contract terms.

## 4) SLA Breach vs Churn Scatter (Network quality vs churn)

- **Measures on FactTelemetry:**
  
  text
  
  `SLA Breach Count = CALCULATE(     COUNTROWS(facttelemetry),    facttelemetry[sla_breach] = TRUE() ) Total Telemetry Rows = COUNTROWS(facttelemetry) SLA Breach Rate % = DIVIDE(     [SLA Breach Count],    [Total Telemetry Rows] ) * 100`

- **Scatter chart fields:**
  
  - X‑axis: `SLA Breach Rate %`.
  
  - Y‑axis: `Churn Rate %`.
  
  - Legend (or Details): `DimCustomer[plan_type]` (or `DimRegion[region]`).

- **Use:** Plots each plan (or region) as a point, showing how higher SLA breach rates correlate with higher churn. This directly ties network/service performance to customer loss, which is a core telco KPI pattern.

---

## Filters / Slicers

The left‑hand empty slot (below the title) is reserved for **page‑level segment filters** so every KPI and visual can be sliced:

Typical slicers you can add there:

- **Date filter:** `DimDate[year]` or `[month]` (could be a Between slicer on date).

- **Region filter:** `DimRegion[region]`.

- **Plan filter:** `DimCustomer[plan_type]`.

- **Tenure filter (optional):** `DimCustomer[Tenure Bucket]`.

All visuals and KPIs use measures built with `CALCULATE` and respect these slicers, giving recruiters the sense of a real operations console rather than static charts.[](https://www.youtube.com/watch?v=LfnJn7Yt7EY)[](https://www.sydle.com/blog/analytics-telecom-68fa8be7a9379014bfeda7a2)

---

## What this dashboard demonstrates (for a recruiter)

- You built a **proper star schema** (dimensional model) on Databricks and exposed it to Power BI.

- You created **business‑relevant KPIs** (churn rate, tenure) and **telecom‑standard visuals** (churn by plan, by region, by tenure, and SLA vs churn).

- You used **DAX** for measures and calculated columns, and carefully aligned them with visual design (transparent KPI cards, Canva background, consistent colours).

- The page supports **strategic decision‑making** for an ISP: which plans are risky, which regions are problematic, at what customer age churn spikes, and how network quality impacts churn.



# Dashboard 2 – Network Performance & SLA

![](C:\Users\Salitha\AppData\Roaming\marktext\images\2026-05-29-20-05-05-image.png)

## Purpose

Dashboard 2 tracks **network quality and SLA performance** for an ISP. It answers:

1. Are we meeting network SLAs over time (latency, packet loss, breaches)?

2. Which regions and plans experience the worst performance?

3. How do bandwidth levels relate to packet loss (quality vs usage)?

4. Where and when are outage incidents occurring?

These are standard telecom network analytics KPIs.

---

## KPIs (Card visuals)

You added four KPI cards across the top, similar styling to Dashboard 1 (transparent cards, white text):

1. **Avg Peak Latency (ms)**
   
   - Measure:
     
     text
     
     `Avg Peak Latency (ms) = AVERAGE ( facttelemetry[peak_hour_latency_ms] )`
   
   - Shows typical peak‑hour latency in milliseconds for the current filter context.

2. **Avg Packet Loss %**
   
   - Measure:
     
     text
     
     `Avg Packet Loss % = AVERAGE ( facttelemetry[packet_loss_percent] )`
   
   - Indicates overall packet loss rate; high values signal poor network quality.[](https://www.telecomhall.net/t/understanding-network-kpis-with-detailed-sub-kpis-real-life-examples/32759)

3. **SLA Breach Rate %**
   
   - Built on `facttelemetry[sla_breach]`:
     
     text
     
     `SLA Breach Count = CALCULATE (     COUNTROWS ( facttelemetry ),    facttelemetry[sla_breach] = TRUE () ) Total Telemetry Rows = COUNTROWS ( facttelemetry ) SLA Breach Rate % = DIVIDE ( [SLA Breach Count], [Total Telemetry Rows] ) * 100`
   
   - Shows percentage of telemetry samples that violated SLA thresholds.

4. **Outages Last 30 Days**
   
   - Assuming `facttelemetry[outage_flag]` and relationship to `DimDate`:
     
     text
     
     `Outages Last 30 Days = VAR MaxDate = MAX ( dimdate[date] ) RETURN CALCULATE (     COUNTROWS ( facttelemetry ),    facttelemetry[outage_flag] = TRUE (),    dimdate[date] > MaxDate - 30 )`
   
   - Counts serious outage events in the last 30 days.

All KPIs respond to slicers (Date, Region, Plan, etc.), making them segment‑aware.

---

## Visualizations

## 1) Network Health Over Time (SLA + Latency)

- **Type:** Line chart.

- **Fields:**
  
  - X‑axis: `DimDate[month]` (or a Month‑Year column).
  
  - Y‑axis lines:
    
    - Line 1: `SLA Breach Rate %`.
    
    - Line 2: `Avg Peak Latency (ms)` (optionally on a secondary axis).

- **Use:** Shows whether network performance is improving or degrading across months; spikes in latency often line up with spikes in SLA breaches.

---

## 2) Avg Peak Latency by Region

- **Type:** Clustered bar chart.

- **Fields:**
  
  - Axis: `DimRegion[region]` (e.g., Sydney, Perth, Canberra, etc.).
  
  - Values: `Avg Peak Latency (ms)`.

- **Use:** Highlights which cities or regions have the worst latency, guiding where to prioritise capacity upgrades or troubleshooting.

---

## 3) Packet Loss vs Bandwidth Scatter (by Plan Type)

- **Supporting measures (optional):**
  
  text
  
  `Avg Bandwidth (GB) = AVERAGE ( facttelemetry[daily_bandwidth_gb] )`
  
  (Or you use the average directly in the visual.)

- **Type:** Scatter chart.

- **Fields:**
  
  - X‑axis: `Avg Bandwidth (GB)` (or `AVERAGE(facttelemetry[daily_bandwidth_gb])`).
  
  - Y‑axis: `Avg Packet Loss %`.
  
  - Legend / Details: `DimCustomer[plan_type]`.

- **Use:** Each dot is a plan; shows whether high‑bandwidth plans are also suffering higher packet loss, a key QoS question for ISPs.

---

## 4) Outage Incidents (Region or Time)

You used the outages metric to visualise severity and distribution.

Two typical patterns (you can implement either, or both on this page or a later one):

- **Outages by Region (bar):**
  
  - Axis: `DimRegion[region]`.
  
  - Values: `Outages Last 30 Days`.
  
  - Shows which regions are most impacted by recent outages.

- **Outages Over Time (area/column):**
  
  - X‑axis: `DimDate[date]` (last N days from slicer).
  
  - Values: count of rows with `outage_flag = TRUE()`.
  
  - Reveals whether outage frequency is trending up or down.

---

## Filters / Slicers (Network‑focused)

On this page you can dedicate a panel (similar to Dashboard 1) to filters for network operations:

- `DimDate[date]` or `[month]` – to focus on a period (e.g., last 30/90 days).

- `DimRegion[region]` – to view KPIs and visuals for a single city or region.

- `DimCustomer[plan_type]` – to compare specific product lines.

- Optional: `DimCustomer[customer_type]` (Business vs Consumer) if you want to distinguish enterprise vs residential traffic.

These slicers control all KPIs and visuals, enabling a Superloop‑style NOC lead to say, for example: “Show me SLA performance and outages for business Fibre plans in Brisbane in the last quarter.”[](https://www.youtube.com/watch?v=LfnJn7Yt7EY)[](https://www.sydle.com/blog/analytics-telecom-68fa8be7a9379014bfeda7a2)

---

## What Dashboard 2 Demonstrates

- You translated raw telemetry (latency, packet loss, outages, SLA flags) into **business‑level KPIs**.

- You built visuals that **mirror standard telecom network dashboards**: SLA trends, latency by region, QoS vs usage, outage tracking.

- You reused the same dimensional model and DAX style from Dashboard 1, showing consistency in modeling and design.

- You designed an interface that supports **strategic decision‑making** for network leaders, not just pretty charts.



# Dashboard 3 – Support Experience & Revenue Risk

Here’s the structured overview of your **Dashboard 3 – Support Experience & Revenue Risk**, in the same style as Dashboards 1 and 2.

---

![](C:\Users\Salitha\AppData\Roaming\marktext\images\2026-05-29-20-06-02-image.png) 

## Purpose

This dashboard connects **support performance** and **billing risk** to customer experience, so an ISP like Superloop can see:

- How big the support load is.

- How effectively issues are being resolved.

- Which issue types and regions drive most of the work.

- How **payment failures and churn** behave by region.

---

## KPIs (Top strip)

From left to right you now show:

1. **Ticket Count**
   
   - Measure: total number of tickets in `FactTickets` for the current filters (date range, region, plan, customer type).
   
   - Communicates overall support workload.

2. **Resolved Ticket Count**
   
   - Measure: count of tickets where `facttickets[resolved] = TRUE()`.
   
   - Shows how many of those tickets have been successfully closed.

3. **Resolved Rate %**
   
   - Measure:
     
     text
     
     `Ticket Count = COUNTROWS ( facttickets ) Resolved Ticket Count = CALCULATE (     COUNTROWS ( facttickets ),    facttickets[resolved] = TRUE () ) Resolved Rate % = DIVIDE ( [Resolved Ticket Count], [Ticket Count] ) * 100`
   
   - Key quality KPI: share of tickets resolved.[](https://www.sydle.com/blog/analytics-telecom-68fa8be7a9379014bfeda7a2)

4. **Payment Failure Rate %**
   
   - Measure based on `FactBilling` and `DimPaymentStatus`:
     
     text
     
     `Failed Payments = CALCULATE (     COUNTROWS ( factbilling ),    dimpaymentstatus[payment_status] = "Failed" ) Total Payments = COUNTROWS ( factbilling ) Payment Failure Rate % = DIVIDE ( [Failed Payments], [Total Payments] ) * 100`
   
   - Bridges support to revenue risk; high values often drive more calls and churn.

All cards use transparent backgrounds and white text over your red/black Canva layout.

---

## Visuals (Middle and Bottom)

## 1) Ticket Counts by Issue Category (top‑left large bar)

- **Type:** Horizontal bar chart.

- **Fields:**
  
  - Axis: `DimIssueCategory[issue_category]` (connectivity_drop, slow_speed, billing_query, etc.).
  
  - Values: `Ticket Count`.

- **Insight:** Shows which issue categories generate the most tickets; connectivity and speed clearly dominate in your screenshot.

---

## 2) Resolved Ticket Count by Issue Category (top‑right bar)

- **Type:** Vertical bar chart.

- **Fields:**
  
  - Axis: `DimIssueCategory[issue_category]`.
  
  - Values: `Resolved Ticket Count`.

- **Insight:** For each issue type, how many tickets have been resolved; lets users see if high‑volume categories are also being cleared effectively.

Placing this next to Ticket Counts by Issue Category creates a natural comparison: volume vs resolved volume for each issue type.

---

## 3) Resolved Rate % and Ticket Count by Region (bottom‑left)

- **Type:** Combo chart (line + clustered column).

- **Fields:**
  
  - Axis: `DimRegion[region]` (e.g., Brisbane, Canberra, Sydney, Melbourne, Adelaide, Perth).
  
  - Columns (left Y): `Ticket Count`.
  
  - Line (right Y): `Resolved Rate %`.

- **Insight:** For each region, you see both workload (ticket count) and quality (resolved rate). A region with many tickets but low resolved rate is an operational hotspot.

---

## 4) Payment Failure Rate % and Churn Rate % by Region (bottom‑right)

- **Type:** Combo or scatter‑style chart.

- **Fields:**
  
  - Axis / X: `DimRegion[region]` **or** `Payment Failure Rate %`.
  
  - Column or dots: `Payment Failure Rate %`.
  
  - Line / second measure: `Churn Rate %` (reused from Dashboard 1, calculated on `DimCustomer[churn_flag]`).

- **Insight:** Shows how payment failure behaviour and churn relate across regions. Regions with higher payment failure rates typically show higher churn, tying support/billing to retention.

The chart in your screenshot labels axes as “Payment Failure Rate %” (X) and “Churn Rate %” (Y), giving one point or bar per region.

---

## Filters / Slicers (Left panel)

You filled the left column with a dedicated filter panel:

1. **Date range slicer** (top):
   
   - Uses `DimDate[date]` with a Between/slider from `1/1/2020` to `12/31/2026`.
   
   - Controls all KPIs and visuals, so you can zoom into any time window.

2. **Region slicer**
   
   - Field: `DimRegion[region]`.
   
   - Option “All” plus individual cities/regions.

3. **Plan Type slicer**
   
   - Field: `DimCustomer[plan_type]`.
   
   - Allows focus on specific products (NBN50, Fibre, Business VoIP, etc.).

4. **Customer Type slicer**
   
   - Field: `DimCustomer[customer_type]` (e.g., Consumer vs Business).

These slicers together make it possible to ask questions like:

“Show me ticket volume, resolution, and payment failure/churn patterns for **Business NBN plans in Sydney in the last quarter**.”

---

## What This Final Dashboard Demonstrates

- A clean, ISP‑relevant page that links **support operations**, **customer experience**, and **billing risk**.

- Solid DAX work for counts, percentages, and cross‑table KPIs.

- Thoughtful layout: title panel, KPI strip, left filter rail, and a 2×2 grid of visuals that each answer a specific strategic question.

- Strong storytelling for recruiters: tickets by issue, resolutions, regional performance, and how failures drive churn.
