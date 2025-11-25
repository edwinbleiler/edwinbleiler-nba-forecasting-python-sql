# NBA Forecasting Archive (Retired)

This repository contains the **original early version** of my NBA forecasting work.

It has been **replaced** by a significantly improved, production-grade pipeline:

👉 **New Project:** https://github.com/edwinbleiler/edwinbleiler-nba-projection-system

This archive remains public for:
- Historical reference  
- Portfolio evolution  
- SEO continuity  
- Code comparison  
- Transparency in my development process  

## Why It’s Archived

This older version lacked:
- Proper modular structure  
- CI/CD automation  
- Robust feature engineering  
- Daily ingestion pipeline  
- Clear ML lifecycle management  

The new system includes all of that and more.

NBA Player Forecasting Project (Python + SQL)

Created by Edwin (Ed) Bleiler

A structured analytics portfolio project using Python, SQL, and a normalized data model.

This repository demonstrates how I build analytical systems: clear data architecture, measurable metrics, repeatable pipelines, and transparent forecasting logic. The project uses NBA player data, but the methods mirror the structured problem-solving I apply in Strategy & Operations, Product, and Chief of Staff roles.

Links:

Website: https://edwinbleiler.com

LinkedIn: https://www.linkedin.com/in/edwin-ed-bleiler/

GitHub: @edwinbleiler

What This Project Demonstrates (High-Level Summary)

This work is intentionally designed to showcase:

Python + SQL proficiency with real data ingestion, schema design, and feature engineering

Data modeling discipline using a normalized relational schema

Forecasting frameworks grounded in interpretable metrics

Operational analytics thinking applied in a structured, repeatable workflow

Product-style clarity around assumptions, logic, and decision-support design

Systems thinking — the same approach I’ve used in cross-functional leadership roles

For SEO: this section gives Google clear, repeated signals about your skills and project domain.

Project Overview

Goal: Build a forecasting workflow that projects NBA player performance using structured metrics and a clean SQL-backed data pipeline.

Tech Stack:
Python | SQL | SQLite | nba_api | pandas | numpy | matplotlib

Outputs:

SQL database (normalized schema)

Feature engineering pipeline in Python

Multi-factor forecast per player

CSV outputs

Visualization + Jupyter notebooks documenting analysis

Methodology
1. Data Ingestion

Pull team, player, game, and box score data via the NBA API

Store normalized tables in SQLite

Maintain raw + processed layers for auditability

2. SQL Schema Design

Tables for teams, players, games, boxscores

Views for derived metrics (FPPG, rolling averages)

Efficient joins for matchup logic and positional relationships

3. Feature Engineering

Metrics include:

Rolling 10-game averages

Consistency score (volatility)

Defensive factor by opponent (DvP)

Composite forecast score

4. Forecasting Logic (simple + interpretable)
base = fppg_last_10
matchup_adj = base * dvp_factor
forecast = matchup_adj * (0.5 + 0.5 * consistency_score / 100)

5. Outputs

outputs/player_forecasts.csv

Notebook-based analysis

Plots showing volatility, matchup factors, and player distributions

Example Output
Player	Team	Avg FP (L10)	Consistency	Opp DvP	Forecast
Player A	BOS	41.2	88	1.05	43.7
Player B	DEN	35.9	76	0.95	34.1
Repository Structure
edwinbleiler-nba-forecasting-python-sql/
├── data/
├── sql/
├── src/
├── notebooks/
├── outputs/
├── PROJECT_PLAN.md
├── LEARNING_LOG.md
├── ABOUT_THIS_PROJECT.md
└── requirements.txt

Skills Demonstrated

Python and SQL for real-world analytics

Data architecture and schema planning

Forecast modeling and metric design

ETL pipelines and automation

Structured operational analytics

Clear communication of logic and assumptions

Systems thinking under ambiguous requirements

About the Author

I’m Edwin (Ed) Bleiler — a Strategy & Operations leader focused on data-driven decision frameworks, scalable systems, and high-clarity execution across Product, Analytics, and cross-functional operations.
This project reflects the same approach I bring to forecasting, vendor evaluations, operational design, and structured problem-solving.

More about me:

https://edwinbleiler.com

https://www.linkedin.com/in/edwin-ed-bleiler/

GitHub: @edwinbleiler
