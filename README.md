This repo aims to solve the following scenario:

Build a data lakehouse that uinified outpatient activities, A&E pressue, inpatient/daycase demand, cross boundary flows, and bed occupation of NHS hospitals.

The project will use databricks to create:

- A unified data lakehouse
- Operational dashboard to show where pressure is building
- Forecasting where demand/occupancy will go next
- Decision Support - what drives overload and where patients come from


## Problem Satement

NHS boards struggle to anticipate and manage demand spikes because outpatient activity, A&E pressure, inpatient/daycase episodes, and staffed-bed occupancy are reported in separate datasets and at different granularities. This leads to delayed interventions, inefficient cross-boundary planning, and reactive bed management.

You need to build a Databricks solution that:

Standardises these datasets into a single analytical model

Enables near real-time reporting and KPI tracking

Produces ML predictions for occupancy/demand and identifies drivers (specialty, SIMD, age/sex mix, cross-boundary flows)


## Deliverables

You will end up with:

Gold “Board Pressure Index” table (single KPI view)

Databricks SQL dashboard for ops + exec

ML models:

Forecast bed occupancy (regression / time series)

Predict risk of “breach” periods (classification)

Production-style pipeline (DLT or Jobs + Delta + Unity Catalog)
