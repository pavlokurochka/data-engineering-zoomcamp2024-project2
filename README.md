# Lightweight Data Pipeline for Company of  Heroes 3 Matches

## Introduction

This is the a second final project for [Data Engineering Zoomcamp 2024](https://github.com/DataTalksClub/data-engineering-zoomcamp). My first project is already accepted. While doing the peer review grading of the submissions by my fellow students, I ran across the [project](https://github.com/KevsDe/de_aoe2_games_data_pipeline) that transformed match stats for Age of Empires 2, which was my favorite game long time ago. This inspired me to start figuring out how to do something similar with the data from my current favorite game [Company of  Heroes 3](https://community.companyofheroes.com/coh-franchise-home/company-of-heroes-3). It turns out the enthusiasts behind [coh3stats](https://coh3stats.com/stats/games) already did the best dashboards I could imagine. However, I thought that there could always be more queries to be run to explore data from some other angle. And while coh3stats guys do store and [expose raw data](https://coh3stats.com/other/open-data), I got the impression that the format is not very data analyst friendly.

## Problem description

This is a proof-of-concept project to build a lightweight and free data engineering infrastructure that would execute a pipeline from downloaded datasets to an analytical dashboard.

We download **fact** data as daily JSON filles with data for multiplayer matches that are stored at [storage.coh3stats.com](https://coh3stats.com/other/open-data). I asked around about **dimensions** data on coh3stats Slack channel and got pointers to some [source](https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-raw-data.ts) [code](https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-data.ts) in their repo. I copied them into [coh3-raw-data.yaml](dlt/coh3-raw-data.yaml).

## Technologies

- **Cloud**: [GitHub codespaces](https://github.com/codespaces).

- **Infrastructure as code (IaC)**: Docker.

- **Workflow orchestration**: [Kestra](https://kestra.io/).

- **Data Warehouse**: [MotherDuck](https://app.motherduck.com/)

- **Batch processing**: [SQLMesh](https://sqlmesh.com/), [dlt](https://dlthub.com/), Python.

- **Dashboard**: [Strimlit](https://cheat-sheet.streamlit.app/)

## Data ingestion (batch) & Workflow orchestration

We have two pipelines run in a Kestra flows in a Kestra docker container that runs on a GitHub codespace.



